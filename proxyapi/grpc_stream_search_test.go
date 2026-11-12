package proxyapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxyapi/mock"
	"github.com/ozontech/seq-db/query"
)

func TestGrpcV1_FinalizeStoreStreams(t *testing.T) {
	tests := []struct {
		name        string
		partialErr  error
		summary     *query.Summary
		hasSuccess  bool
		failPartial bool
		check       func(t *testing.T, got *query.Summary, err error)
	}{
		{
			name:       "no_errors",
			summary:    &query.Summary{Total: 5},
			hasSuccess: true,
			check: func(t *testing.T, got *query.Summary, err error) {
				require.NoError(t, err)
				assert.Equal(t, uint64(5), got.Total)
				assert.NoError(t, got.Err)
			},
		},
		{
			name:       "too_many_fractions_hit",
			summary:    &query.Summary{Err: consts.ErrTooManyFractionsHit},
			hasSuccess: true,
			check: func(t *testing.T, got *query.Summary, err error) {
				require.NoError(t, err)
				assert.ErrorIs(t, got.Err, consts.ErrTooManyFractionsHit)
				assert.Equal(t, seqproxyapi.ErrorCode_ERROR_CODE_TOO_MANY_FRACTIONS_HIT, mapProxyErrorCode(got.Err))
			},
		},
		{
			name:       "wants_old_data",
			summary:    &query.Summary{Err: consts.ErrIngestorQueryWantsOldData},
			hasSuccess: true,
			check: func(t *testing.T, _ *query.Summary, err error) {
				require.Error(t, err)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name:       "hard_error_without_successful_stream",
			summary:    &query.Summary{Err: errors.New("store stream closed without summary")},
			hasSuccess: false,
			check: func(t *testing.T, _ *query.Summary, err error) {
				require.Error(t, err)
				assert.Equal(t, codes.Internal, status.Code(err))
			},
		},
		{
			name:        "hard_error_with_fail_partial",
			summary:     &query.Summary{Err: errors.New("memory limit exceeded")},
			hasSuccess:  true,
			failPartial: true,
			check: func(t *testing.T, _ *query.Summary, err error) {
				require.Error(t, err)
				assert.Equal(t, codes.Internal, status.Code(err))
			},
		},
		{
			name:       "hard_error_with_successful_stream",
			summary:    &query.Summary{Err: errors.New("memory limit exceeded")},
			hasSuccess: true,
			check: func(t *testing.T, got *query.Summary, err error) {
				require.NoError(t, err)
				require.Error(t, got.Err)
				assert.ErrorIs(t, got.Err, consts.ErrPartialResponse)
				assert.ErrorContains(t, got.Err, "memory limit exceeded")
			},
		},
		{
			name:       "partial_from_open",
			partialErr: fmt.Errorf("%w: shard is dead", consts.ErrPartialResponse),
			summary:    &query.Summary{Total: 3},
			hasSuccess: true,
			check: func(t *testing.T, got *query.Summary, err error) {
				require.NoError(t, err)
				assert.Equal(t, uint64(3), got.Total)
				assert.ErrorIs(t, got.Err, consts.ErrPartialResponse)
				assert.Equal(t, seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE, mapProxyErrorCode(got.Err))
			},
		},
		{
			name:        "partial_from_open_with_fail_partial",
			partialErr:  fmt.Errorf("%w: shard is dead", consts.ErrPartialResponse),
			summary:     &query.Summary{Total: 3},
			hasSuccess:  true,
			failPartial: true,
			check: func(t *testing.T, _ *query.Summary, err error) {
				require.Error(t, err)
				assert.Equal(t, codes.Internal, status.Code(err))
			},
		},
		{
			name:       "partial_with_soft_error",
			partialErr: fmt.Errorf("%w: shard is dead", consts.ErrPartialResponse),
			summary:    &query.Summary{Err: consts.ErrTooManyFractionsHit},
			hasSuccess: true,
			check: func(t *testing.T, got *query.Summary, err error) {
				require.NoError(t, err)
				assert.ErrorIs(t, got.Err, consts.ErrTooManyFractionsHit)
				assert.Equal(t, seqproxyapi.ErrorCode_ERROR_CODE_TOO_MANY_FRACTIONS_HIT, mapProxyErrorCode(got.Err))
			},
		},
		{
			name:       "partial_when_all_streams_failed",
			partialErr: fmt.Errorf("%w: shard is dead", consts.ErrPartialResponse),
			summary:    &query.Summary{Err: errors.New("store stream closed without summary")},
			hasSuccess: false,
			check: func(t *testing.T, _ *query.Summary, err error) {
				require.Error(t, err)
				assert.Equal(t, codes.Internal, status.Code(err))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newGrpcV1(APIConfig{}, nil, nil, nil, nil)
			ss := search.NewStreamResult(
				&fakeProducer{summary: tc.summary},
				func() {},
				tc.partialErr,
				func() bool { return tc.hasSuccess },
			)

			got, err := g.finalizeStoreStreams(failPartialCtx(tc.failPartial), ss)
			tc.check(t, got, err)
		})
	}
}

func TestGrpcV1_OpenStoreStreams(t *testing.T) {
	tests := []struct {
		name               string
		storesStreamResult *search.StreamResult
		storesStreamErr    error
		failPartial        bool
		wantErrCode        codes.Code
		wantResult         bool
		wantCancelled      bool
	}{
		{
			name:            "hard_error",
			storesStreamErr: errors.New("can't open stream: connection refused"),
			wantErrCode:     codes.Internal,
		},
		{
			name:            "wants_old_data",
			storesStreamErr: fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData),
			wantErrCode:     codes.InvalidArgument,
		},
		{
			name: "fail_partial",
			storesStreamResult: &search.StreamResult{
				Records:    &fakeProducer{},
				PartialErr: fmt.Errorf("%w: shard is dead", consts.ErrPartialResponse),
			},
			failPartial:   true,
			wantErrCode:   codes.Internal,
			wantCancelled: true,
		},
		{
			name: "partial",
			storesStreamResult: &search.StreamResult{
				Records:    &fakeProducer{},
				PartialErr: fmt.Errorf("%w: shard is dead", consts.ErrPartialResponse),
			},
			wantResult: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			cancelled := false
			if tc.storesStreamResult != nil {
				tc.storesStreamResult.Cancel = func() { cancelled = true }
			}

			siMock := mock.NewMockSearchIngestor(ctrl)
			siMock.EXPECT().
				StreamSearch(gomock.Any(), &search.StreamSearchRequest{}, gomock.Any()).
				Return(tc.storesStreamResult, tc.storesStreamErr).
				Times(1)

			g := newGrpcV1(APIConfig{}, siMock, nil, nil, nil)

			got, err := g.openStoreStreams(failPartialCtx(tc.failPartial), &search.StreamSearchRequest{}, nil)

			if tc.wantErrCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tc.wantErrCode, status.Code(err))
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, tc.storesStreamResult, got)
			}
			assert.Equal(t, tc.wantCancelled, cancelled)
		})
	}
}

type fakeProducer struct {
	summary *query.Summary
}

func (f *fakeProducer) Next() *query.Record { return nil }

func (f *fakeProducer) Finalize() *query.Summary { return f.summary }

func failPartialCtx(fail bool) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("fail-partial-response", strconv.FormatBool(fail)))
}

type emptyStream struct {
	grpc.ServerStream
}

func (s emptyStream) Context() context.Context { return context.Background() }

func (s emptyStream) Send(*seqproxyapi.StreamSearchResponse) error { return nil }

func (s emptyStream) Recv() (*seqproxyapi.StreamSearchRequest, error) {
	return nil, io.EOF
}

func TestGrpcV1_StreamSearchEmptyRequest(t *testing.T) {
	g := newGrpcV1(APIConfig{}, nil, nil, nil, nil)

	err := g.StreamSearch(emptyStream{})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
