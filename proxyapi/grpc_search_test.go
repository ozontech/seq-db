package proxyapi

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
)

type searchTestCaseData struct {
	searchQ   *testSearchQuery
	size      int64
	offset    int64
	withTotal bool

	partial bool
	respErr *seqproxyapi.Error
	noResp  bool

	noSiMock bool
	noRlMock bool

	siErr       error
	qprErr      []seq.ErrorSource
	rateLimited bool
}

type searchTestData struct {
	req   *seqproxyapi.SearchRequest
	want  *seqproxyapi.SearchResponse
	mData *mocksData
}

func checkSearchQueries(t *testing.T, cData searchTestCaseData) {
	if cData.searchQ == nil {
		t.Fatal("search query must be provided")
	}
}

func prepareSearchTestData(t *testing.T, cData searchTestCaseData) searchTestData {
	req := &seqproxyapi.SearchRequest{
		Size:      cData.size,
		Offset:    cData.offset,
		WithTotal: cData.withTotal,
	}
	if cData.searchQ != nil {
		req.Query = &seqproxyapi.SearchQuery{
			Query:   cData.searchQ.query,
			From:    &timestamppb.Timestamp{Seconds: cData.searchQ.from.Unix()},
			To:      &timestamppb.Timestamp{Seconds: cData.searchQ.to.Unix() + 1},
			Explain: cData.searchQ.explain,
		}
	}

	var resp *seqproxyapi.SearchResponse
	var docs search.DocsIterator = search.EmptyDocsStream{}

	qpr := &seq.QPR{
		Errors: cData.qprErr,
	}
	if !cData.noResp {
		checkSearchQueries(t, cData)
		sRespData := makeSearchRespData(int(cData.size))
		docs = newSliceDocsStream(sRespData.ids, sRespData.docs)
		qpr.IDs = sRespData.ids
		qpr.Total = uint64(len(sRespData.ids))
		resp = &seqproxyapi.SearchResponse{
			Total:           int64(len(sRespData.docs)),
			Docs:            sRespData.respDocs,
			PartialResponse: cData.partial,
			Error:           cData.respErr,
		}
	}

	var siSearchMock *siSearchMockData
	if !cData.noSiMock {
		checkSearchQueries(t, cData)
		sr := &search.SearchRequest{
			Explain:     req.Query.Explain,
			Q:           []byte(req.Query.Query),
			Size:        int(req.Size),
			Offset:      int(req.Offset),
			From:        seq.MID(req.Query.From.AsTime().UnixMilli()),
			To:          seq.MID(req.Query.To.AsTime().UnixMilli()),
			WithTotal:   req.WithTotal,
			ShouldFetch: true,
		}
		siSearchMock = &siSearchMockData{
			sr: sr,
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
		checkSearchQueries(t, cData)
		rlMock = &rlMockData{
			query:   req.Query.Query,
			limited: cData.rateLimited,
		}
	}

	return searchTestData{
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

func TestGrpcV1_Search(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		data    searchTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: searchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
				respErr: &seqproxyapi.Error{
					Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
				},
			},
			wantErr: false,
		},
		{
			name: "partial_resp",
			data: searchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:partial",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:    10,
				offset:  0,
				partial: true,
				respErr: &seqproxyapi.Error{
					Code:    seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE,
					Message: consts.ErrPartialResponse.Error(),
				},
				siErr: consts.ErrPartialResponse,
			},
			wantErr: false,
		},
		{
			name: "ingestor_err",
			data: searchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ingestor-err",
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
			name: "qpr_err",
			data: searchTestCaseData{
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
			name: "old_data_err",
			data: searchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:old-data-err",
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
			name: "rate_limited_err",
			data: searchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ratelimited",
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
			name: "nil_search_query_err",
			data: searchTestCaseData{
				noResp:   true,
				noSiMock: true,
				noRlMock: true,
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
			defer ctrl.Finish()

			testData := prepareSearchTestData(t, tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			got, err := a.s.Search(a.ctx, testData.req)
			r.Equal(tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			r.NotNil(got)
			r.Equal(testData.want.PartialResponse, got.PartialResponse)
			r.Equal(testData.want.Error, got.Error)
			r.Equal(testData.want.Total, got.Total)
			r.Equal(len(testData.want.Docs), len(got.Docs))
			wantDocsMap := make(map[string][]byte, len(testData.want.Docs))
			for _, doc := range testData.want.Docs {
				wantDocsMap[doc.Id] = doc.Data
			}
			for _, doc := range got.Docs {
				wantDoc, ok := wantDocsMap[doc.Id]
				r.True(ok)
				r.Equal(wantDoc, doc.Data)
			}
		})
	}
}
