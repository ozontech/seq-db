package proxyapi

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxyapi/mock"
	"github.com/ozontech/seq-db/seq"
)

type exportTestCaseData struct {
	searchQ *testSearchQuery
	size    int64
	offset  int64

	noResp      bool
	noSiMock    bool
	noRlMock    bool
	rateLimited bool

	siErr         error
	qprErr        []seq.ErrorSource
	streamSendErr bool
}

type exportTestData struct {
	req   *seqproxyapi.ExportRequest
	want  []*seqproxyapi.ExportResponse
	mData *mocksData
}

func prepareExportTestData(cData exportTestCaseData) exportTestData {
	req := &seqproxyapi.ExportRequest{
		Size:   cData.size,
		Offset: cData.offset,
	}
	if cData.searchQ != nil {
		req.Query = &seqproxyapi.SearchQuery{
			Query:   cData.searchQ.query,
			From:    timestamppb.New(cData.searchQ.from),
			To:      timestamppb.New(cData.searchQ.to),
			Explain: cData.searchQ.explain,
		}
	}

	var resp []*seqproxyapi.ExportResponse
	var docs search.DocsIterator = search.EmptyDocsStream{}
	qpr := &seq.QPR{
		Errors: cData.qprErr,
	}
	if !cData.noResp {
		sRespData := makeExportRespData(int(cData.size))
		docs = newSliceDocsStream(sRespData.ids, sRespData.docs)
		qpr.IDs = sRespData.ids
		qpr.Total = uint64(len(sRespData.ids))
		resp = sRespData.resp
	}

	var siSearchMock *siSearchMockData
	if !cData.noSiMock {
		siSearchMock = &siSearchMockData{
			sr: &search.SearchRequest{
				Explain:     req.Query.Explain,
				Q:           []byte(req.Query.Query),
				Offset:      int(req.Offset),
				Size:        int(req.Size),
				From:        seq.MID(req.Query.From.AsTime().UnixMilli()),
				To:          seq.MID(req.Query.To.AsTime().UnixMilli()),
				ShouldFetch: true,
			},
			ret: siSearchRet{
				qpr:  qpr,
				docs: docs,
				took: time.Second,
				err:  cData.siErr,
			},
		}
	}

	var rlMock *rlMockData
	if !cData.noRlMock {
		rlMock = &rlMockData{
			query:   req.Query.Query,
			limited: cData.rateLimited,
		}
	}

	return exportTestData{
		req:  req,
		want: resp,
		mData: &mocksData{
			si: &siMockData{
				search: siSearchMock,
			},
			rl: rlMock,
		},
	}
}

func TestGrpcV1_Export(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		data    exportTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
			},
			wantErr: false,
		},
		{
			name: "empty_search_query",
			data: exportTestCaseData{
				noResp:   true,
				noSiMock: true,
				noRlMock: true,
			},
			wantErr: true,
		},
		{
			name: "qpr_err",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:qpr-err",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
				noResp: true,
				qprErr: []seq.ErrorSource{
					{ErrStr: "test err", Source: 0},
				},
			},
			wantErr: true,
		},
		{
			name: "si_err",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:si-err",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
				noResp: true,
				siErr:  errors.New("test"),
			},
			wantErr: true,
		},
		{
			name: "si_err_old_data",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:si-err-old-data",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
				noResp: true,
				siErr:  consts.ErrIngestorQueryWantsOldData,
			},
			wantErr: true,
		},
		{
			name: "rate_limited",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:rate-limited",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:        10,
				offset:      0,
				noResp:      true,
				noSiMock:    true,
				rateLimited: true,
			},
			wantErr: true,
		},
		{
			name: "stream_send_err",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:stream-send-err",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:          10,
				offset:        0,
				streamSendErr: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)

			testData := prepareExportTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			ctx := context.Background()
			ctx, span := trace.StartSpan(
				ctx, "export-test", trace.WithSampler(trace.AlwaysSample()),
			)
			defer span.End()

			streamMock := mock.NewMockExportServer(ctrl)
			streamMock.EXPECT().Send(gomock.Any()).DoAndReturn(
				func(_ *seqproxyapi.ExportResponse) error {
					if tt.data.streamSendErr {
						return errors.New("test-send-error")
					}
					return nil
				},
			).AnyTimes()
			streamMock.EXPECT().Context().Return(ctx).AnyTimes()

			err := a.s.Export(testData.req, streamMock)
			r.Equal(tt.wantErr, err != nil)
		})
	}
}

func TestGrpcV1_ExportLive(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		data    exportTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
			},
			wantErr: false,
		},
		{
			name: "max_requested_documents",
			data: exportTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:max_req_docs",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:     int64(config.MaxRequestedDocuments) + 10,
				offset:   0,
				noSiMock: true,
				noRlMock: true,
				noResp:   true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)

			testData := prepareExportTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			client, closer := runGRPCServerWithClient(a.s)
			defer closer()

			out, err := client.Export(a.ctx, testData.req)
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
