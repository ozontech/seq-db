package proxyapi

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxyapi/mock"
)

type exportAsyncTestCaseData struct {
	searchID string
	size     int64
	offset   int64

	// maxDocs overrides config.AsyncSearchMaxDocumentsPerRequest; 0 means no limit.
	maxDocs int64

	noResp bool

	fetchErr      error
	streamSendErr bool
}

type exportAsyncTestData struct {
	req   *seqproxyapi.ExportAsyncSearchRequest
	want  []*seqproxyapi.ExportResponse
	fetch search.FetchAsyncSearchResultRequest
	docs  search.DocsIterator
}

func prepareExportAsyncTestData(cData exportAsyncTestCaseData) exportAsyncTestData {
	req := &seqproxyapi.ExportAsyncSearchRequest{
		SearchId: cData.searchID,
		Size:     cData.size,
		Offset:   cData.offset,
	}

	var resp []*seqproxyapi.ExportResponse
	var docs search.DocsIterator = search.EmptyDocsStream{}
	if !cData.noResp {
		sRespData := makeExportRespData(int(cData.size))
		docs = newSliceDocsStream(sRespData.ids, sRespData.docs)
		resp = sRespData.resp
	}

	return exportAsyncTestData{
		req:  req,
		want: resp,
		fetch: search.FetchAsyncSearchResultRequest{
			ID:     req.SearchId,
			Size:   int(req.Size),
			Offset: int(req.Offset),
		},
		docs: docs,
	}
}

func TestGrpcV1_ExportAsyncSearch(t *testing.T) {
	tests := []struct {
		name    string
		data    exportAsyncTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: exportAsyncTestCaseData{
				searchID: "test-id-ok",
				size:     10,
				offset:   0,
			},
			wantErr: false,
		},
		{
			name: "empty_docs",
			data: exportAsyncTestCaseData{
				searchID: "test-id-empty",
				size:     10,
				offset:   0,
				noResp:   true,
			},
			wantErr: false,
		},
		{
			name: "fetch_err",
			data: exportAsyncTestCaseData{
				searchID: "test-id-fetch-err",
				size:     10,
				offset:   0,
				noResp:   true,
				fetchErr: errors.New("test"),
			},
			wantErr: true,
		},
		{
			name: "too_many_documents",
			data: exportAsyncTestCaseData{
				searchID: "test-id-too-many",
				size:     20,
				offset:   0,
				maxDocs:  10,
				noResp:   true,
			},
			wantErr: true,
		},
		{
			name: "stream_send_err",
			data: exportAsyncTestCaseData{
				searchID:      "test-id-stream-send-err",
				size:          10,
				offset:        0,
				streamSendErr: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)

			testData := prepareExportAsyncTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, &mocksData{})
			a.s.config.AsyncSearchMaxDocumentsPerRequest = tt.data.maxDocs

			// FetchAsyncSearchResult is only expected when the request passes
			// the size limit check.
			if tt.data.maxDocs == 0 || tt.data.size <= tt.data.maxDocs {
				a.m.siMock.EXPECT().
					FetchAsyncSearchResult(gomock.Any(), testData.fetch).
					Return(search.FetchAsyncSearchResultResponse{}, testData.docs, tt.data.fetchErr)
			}

			ctx := context.Background()
			ctx, span := trace.StartSpan(
				ctx, "async-export-test", trace.WithSampler(trace.AlwaysSample()),
			)
			defer span.End()

			streamMock := mock.NewMockExportAsyncSearchServer(ctrl)
			streamMock.EXPECT().Send(gomock.Any()).DoAndReturn(
				func(_ *seqproxyapi.ExportResponse) error {
					if tt.data.streamSendErr {
						return errors.New("test-send-error")
					}
					return nil
				},
			).AnyTimes()
			streamMock.EXPECT().Context().Return(ctx).AnyTimes()

			err := a.s.ExportAsyncSearch(testData.req, streamMock)
			r.Equal(tt.wantErr, err != nil)
		})
	}
}

func TestGrpcV1_ExportAsyncSearchLive(t *testing.T) {
	tests := []struct {
		name    string
		data    exportAsyncTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: exportAsyncTestCaseData{
				searchID: "test-id-ok",
				size:     10,
				offset:   0,
			},
			wantErr: false,
		},
		{
			name: "empty_docs",
			data: exportAsyncTestCaseData{
				searchID: "test-id-empty",
				size:     10,
				offset:   0,
				noResp:   true,
			},
			wantErr: false,
		},
		{
			name: "fetch_err",
			data: exportAsyncTestCaseData{
				searchID: "test-id-fetch-err",
				size:     10,
				offset:   0,
				noResp:   true,
				fetchErr: errors.New("test"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)

			testData := prepareExportAsyncTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, &mocksData{})

			a.m.siMock.EXPECT().
				FetchAsyncSearchResult(gomock.Any(), testData.fetch).
				Return(search.FetchAsyncSearchResultResponse{}, testData.docs, tt.data.fetchErr)

			client, closer := runGRPCServerWithClient(a.s)
			defer closer()

			out, err := client.ExportAsyncSearch(a.ctx, testData.req)
			r.NoError(err)

			var recvErr error
			got := make([]*seqproxyapi.ExportResponse, 0)
			for {
				o, err := out.Recv()
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					recvErr = err
					break
				}
				got = append(got, o)
			}

			r.Equal(tt.wantErr, recvErr != nil)
			if tt.wantErr {
				return
			}
			r.Equal(len(testData.want), len(got))
			for i := 0; i < len(got); i++ {
				r.Equal(testData.want[i].Doc.Id, got[i].Doc.Id)
				r.Equal(testData.want[i].Doc.Data, got[i].Doc.Data)
			}
		})
	}
}
