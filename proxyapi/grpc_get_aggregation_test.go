package proxyapi

import (
	"errors"
	"strings"
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

type getAggregationTestCaseData struct {
	searchQ   *testSearchQuery
	aggQ      []*testAggQuery
	totalSize int64

	partial bool
	respErr *seqproxyapi.Error
	noResp  bool

	noSiMock bool
	noRlMock bool

	siErr       error
	qprErr      []seq.ErrorSource
	rateLimited bool
}

type getAggregationTestData struct {
	req   *seqproxyapi.GetAggregationRequest
	want  *seqproxyapi.GetAggregationResponse
	mData *mocksData
}

func checkGetAggQueries(t *testing.T, cData getAggregationTestCaseData) {
	if cData.searchQ == nil {
		t.Fatal("search query must be provided")
	}
	if len(cData.aggQ) == 0 {
		t.Fatal("agg query must be provided")
	}
}

func prepareGetAggregationTestData(t *testing.T, cData getAggregationTestCaseData) getAggregationTestData {
	req := &seqproxyapi.GetAggregationRequest{}
	if cData.searchQ != nil {
		req.Query = &seqproxyapi.SearchQuery{
			Query:   cData.searchQ.query,
			From:    &timestamppb.Timestamp{Seconds: cData.searchQ.from.Unix()},
			To:      &timestamppb.Timestamp{Seconds: cData.searchQ.to.Unix() + 1},
			Explain: cData.searchQ.explain,
		}
	}
	if len(cData.aggQ) > 0 {
		for _, query := range cData.aggQ {
			req.Aggs = append(req.Aggs, &seqproxyapi.AggQuery{
				Field: query.aggField,
			})
		}
	}

	var resp *seqproxyapi.GetAggregationResponse
	qpr := &seq.QPR{Errors: cData.qprErr}
	if !cData.noResp {
		checkGetAggQueries(t, cData)
		resp = &seqproxyapi.GetAggregationResponse{
			Total:           cData.totalSize,
			PartialResponse: cData.partial,
			Error:           cData.respErr,
		}
		if len(cData.aggQ) > 0 {
			qpr.Aggs = make([]seq.QPRHistogram, len(cData.aggQ))
			resp.Aggs = make([]*seqproxyapi.Aggregation, len(cData.aggQ))
			for i, query := range cData.aggQ {
				aRespData := makeGetAggRespData(cData.totalSize, query.bucketsCnt)
				qpr.Aggs[i].Merge(seq.QPRHistogram{HistogramByToken: aRespData.qprAgg})
				resp.Aggs[i] = aRespData.apiAgg
			}
		}
	}

	var siSearchMock *siSearchMockData
	if !cData.noSiMock {
		checkGetAggQueries(t, cData)
		sr := &search.SearchRequest{
			Explain: req.Query.Explain,
			Q:       []byte(req.Query.Query),
			From:    seq.MID(req.Query.From.AsTime().UnixMilli()),
			To:      seq.MID(req.Query.To.AsTime().UnixMilli()),
		}
		if len(cData.aggQ) > 0 {
			for _, query := range cData.aggQ {
				sr.AggQ = append(sr.AggQ, search.AggQuery{
					Field: query.aggField,
				})
			}
		}
		siSearchMock = &siSearchMockData{
			sr: sr,
			ret: siSearchRet{
				qpr:  qpr,
				took: time.Second,
				err:  cData.siErr,
			},
		}
	}
	var rlMock *rlMockData
	if !cData.noRlMock {
		checkGetAggQueries(t, cData)
		rlQuery := []string{req.Query.Query}
		for _, q := range req.Aggs {
			rlQuery = append(rlQuery, q.Field, q.GroupBy, q.Func.String())
		}
		rlMock = &rlMockData{
			query:   strings.Join(rlQuery, ","),
			limited: cData.rateLimited,
		}
	}

	return getAggregationTestData{
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

func TestGrpcV1_GetAggregation(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		data    getAggregationTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				respErr: &seqproxyapi.Error{
					Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
				},
			},
			wantErr: false,
		},
		{
			name: "ok_multiagg",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{
					{
						aggField:   "test",
						bucketsCnt: 10,
					},
					{
						aggField:   "test2",
						bucketsCnt: 20,
					},
				},
				respErr: &seqproxyapi.Error{
					Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
				},
			},
			wantErr: false,
		},
		{
			name: "partial_resp",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:partial",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
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
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ingestor-err",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				noResp: true,
				siErr:  errors.New("test"),
			},
			wantErr: true,
		},
		{
			name: "qpr_err",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:qpr-err",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				noResp: true,
				qprErr: []seq.ErrorSource{
					{ErrStr: "test err", Source: 0},
				},
			},
			wantErr: true,
		},
		{
			name: "old_data_err",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:old-data-err",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				noResp: true,
				siErr:  consts.ErrIngestorQueryWantsOldData,
			},
			wantErr: true,
		},
		{
			name: "rate_limited_err",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ratelimited",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				noResp:      true,
				noSiMock:    true,
				rateLimited: true,
			},
			wantErr: true,
		},
		{
			name: "nil_search_query_err",
			data: getAggregationTestCaseData{
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				noResp:   true,
				noSiMock: true,
				noRlMock: true,
			},
			wantErr: true,
		},
		{
			name: "nil_agg_query_err",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:nil-agg-q",
					from:  now,
					to:    now.Add(time.Second),
				},
				noResp:   true,
				noSiMock: true,
				noRlMock: true,
			},
			wantErr: true,
		},
		{
			name: "invalid_agg_query_err",
			data: getAggregationTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:invalid-agg-query",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ:     []*testAggQuery{{}},
				noResp:   true,
				noSiMock: true,
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

			testData := prepareGetAggregationTestData(t, tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			got, err := a.s.GetAggregation(a.ctx, testData.req)
			r.Equal(tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			r.NotNil(got)
			r.Equal(testData.want.PartialResponse, got.PartialResponse)
			r.Equal(testData.want.Error, got.Error)
			r.Equal(testData.want.Total, got.Total)
			if len(testData.want.Aggs) == 0 {
				r.Empty(got.Aggs)
				return
			}
			// check data prepared correctly
			r.Equal(len(testData.req.Aggs), len(testData.want.Aggs))
			r.Equal(len(testData.req.Aggs), len(got.Aggs))
			for i := 0; i < len(testData.want.Aggs); i++ {
				wantBuckets := testData.want.Aggs[i].Buckets
				gotBuckets := got.Aggs[i].Buckets
				r.Equal(len(wantBuckets), len(gotBuckets))
				wantBucketsMap := make(map[string]uint64)
				for _, bucket := range wantBuckets {
					wantBucketsMap[bucket.Key] = uint64(bucket.Value)
				}
				for _, bucket := range gotBuckets {
					wantDocCount, ok := wantBucketsMap[bucket.Key]
					r.True(ok)
					r.Equal(wantDocCount, uint64(bucket.Value))
				}
			}
		})
	}
}
