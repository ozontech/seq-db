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
	"github.com/ozontech/seq-db/util"
)

type cSearchTestCaseData struct {
	searchQ      *testSearchQuery
	searchQQuery string
	histQ        *testHistQuery
	aggQ         []*testAggQuery
	size         int64
	offset       int64
	withTotal    bool
	totalSize    int64

	partial bool
	respErr *seqproxyapi.Error
	noResp  bool

	noSiMock bool
	noRlMock bool

	siErr       error
	qprErr      []seq.ErrorSource
	rateLimited bool
}

type cSearchTestData struct {
	req   *seqproxyapi.ComplexSearchRequest
	want  *seqproxyapi.ComplexSearchResponse
	mData *mocksData
}

func checkCSearchQueries(t *testing.T, cData cSearchTestCaseData) {
	if cData.searchQ == nil {
		t.Fatal("search query must be provided")
	}
}

func prepareComplexSearchTestData(t *testing.T, cData cSearchTestCaseData) cSearchTestData {
	req := &seqproxyapi.ComplexSearchRequest{
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
	} else if cData.searchQQuery != "" {
		req.Query = &seqproxyapi.SearchQuery{
			Query: cData.searchQQuery,
		}
	}
	if len(cData.aggQ) > 0 {
		for _, query := range cData.aggQ {
			req.Aggs = append(req.Aggs, &seqproxyapi.AggQuery{
				Field: query.aggField,
			})
		}
	}
	if cData.histQ != nil {
		req.Hist = &seqproxyapi.HistQuery{
			Interval: cData.histQ.interval,
		}
	}

	var resp *seqproxyapi.ComplexSearchResponse
	var docs search.DocsIterator = search.EmptyDocsStream{}

	qpr := &seq.QPR{Errors: cData.qprErr}
	if !cData.noResp {
		checkCSearchQueries(t, cData)
		sRespData := makeSearchRespData(int(cData.size))
		docs = newSliceDocsStream(sRespData.ids, sRespData.docs)
		qpr.IDs = sRespData.ids
		qpr.Total = uint64(cData.totalSize)
		resp = &seqproxyapi.ComplexSearchResponse{
			Total:           cData.totalSize,
			Docs:            sRespData.respDocs,
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
		if cData.histQ != nil {
			hRespData, err := makeGetHistRespData(
				cData.histQ.interval,
				cData.totalSize,
				cData.searchQ.from.UnixMilli(),
				cData.searchQ.to.UnixMilli(),
			)
			if err != nil {
				t.Fatal(err.Error())
			}
			qpr.Histogram = hRespData.qprHist
			resp.Hist = hRespData.apiHist
		}
	}

	var siSearchMock *siSearchMockData
	if !cData.noSiMock {
		checkCSearchQueries(t, cData)
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
		if len(cData.aggQ) > 0 {
			for _, aggQuery := range cData.aggQ {
				sr.AggQ = append(sr.AggQ, search.AggQuery{
					Field: aggQuery.aggField,
				})
			}
		}
		if cData.histQ != nil {
			interval := req.Hist.Interval
			intervalDur, err := util.ParseDuration(interval)
			if err != nil {
				t.Fatalf("failed to parse histogram interval duration: %q", interval)
			}
			sr.Interval = seq.DurationToMID(intervalDur)
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
		checkCSearchQueries(t, cData)
		rlQuery := []string{req.Query.Query}
		if req.Aggs != nil {
			for _, q := range req.Aggs {
				rlQuery = append(rlQuery, q.Field, q.GroupBy, q.Func.String())
			}
		}
		if req.Hist != nil {
			rlQuery = append(rlQuery, req.Hist.Interval)
		}
		rlMock = &rlMockData{
			query:   strings.Join(rlQuery, ","),
			limited: cData.rateLimited,
		}
	}

	respErr := cData.respErr
	if respErr != nil && !shouldHaveResponse(respErr.Code) {
		resp = &seqproxyapi.ComplexSearchResponse{
			Error: cData.respErr,
		}
	}

	return cSearchTestData{
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

func TestGrpcV1_ComplexSearch(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		data    cSearchTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: cSearchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Hour),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				histQ: &testHistQuery{
					interval: "6m",
				},
				size:      10,
				offset:    0,
				totalSize: 1000,
				respErr: &seqproxyapi.Error{
					Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
				},
			},
			wantErr: false,
		},
		{
			name: "ok_multiagg",
			data: cSearchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Hour),
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
				histQ: &testHistQuery{
					interval: "6m",
				},
				size:      10,
				offset:    0,
				totalSize: 1000,
				respErr: &seqproxyapi.Error{
					Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
				},
			},
			wantErr: false,
		},
		{
			name: "partial_resp",
			data: cSearchTestCaseData{
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
			data: cSearchTestCaseData{
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
			data: cSearchTestCaseData{
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
			data: cSearchTestCaseData{
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
			name: "hist_interval_err",
			data: cSearchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:hist-interval-err",
					from:  now,
					to:    now.Add(time.Hour),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				histQ: &testHistQuery{
					interval: "error",
				},
				size:      10,
				offset:    0,
				totalSize: 1000,
				noResp:    true,
				noSiMock:  true,
			},
			wantErr: true,
		},
		{
			name: "rate_limited_err",
			data: cSearchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ratelimited",
					from:  now,
					to:    now.Add(time.Hour),
				},
				aggQ: []*testAggQuery{{
					aggField:   "test",
					bucketsCnt: 10,
				}},
				histQ: &testHistQuery{
					interval: "10s",
				},
				size:        10,
				offset:      0,
				totalSize:   1000,
				noResp:      true,
				noSiMock:    true,
				rateLimited: true,
			},
			wantErr: true,
		},
		{
			name: "nil_search_query_err",
			data: cSearchTestCaseData{
				noResp:   true,
				noSiMock: true,
				noRlMock: true,
			},
			wantErr: true,
		},
		{
			name: "nil_from_err",
			data: cSearchTestCaseData{
				searchQQuery: "test",
				noResp:       true,
				noSiMock:     true,
				noRlMock:     true,
			},
			wantErr: true,
		},
		{
			name: "invalid_agg_query_err",
			data: cSearchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:invalid-agg-query",
					from:  now,
					to:    now.Add(time.Second),
				},
				aggQ:     []*testAggQuery{{}},
				size:     10,
				offset:   0,
				noResp:   true,
				noSiMock: true,
			},
			wantErr: true,
		},
		{
			name: "too_many_fractions_hit",
			data: cSearchTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:too-many-fractions-hit",
					from:  now,
					to:    now.Add(time.Second),
				},
				size:   10,
				offset: 0,
				noResp: true,
				siErr:  consts.ErrTooManyFractionsHit,
				respErr: &seqproxyapi.Error{
					Code:    seqproxyapi.ErrorCode_ERROR_CODE_TOO_MANY_FRACTIONS_HIT,
					Message: "too many fractions hit",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			testData := prepareComplexSearchTestData(t, tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			got, err := a.s.ComplexSearch(a.ctx, testData.req)
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

			if len(testData.want.Aggs) > 0 {
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
			} else {
				r.Nil(got.Aggs)
			}

			if testData.want.Hist != nil {
				wantBuckets := testData.want.Hist.Buckets
				gotBuckets := got.Hist.Buckets
				r.Equal(len(wantBuckets), len(gotBuckets))
				wantBucketsMap := make(map[uint64]uint64, len(wantBuckets))
				for _, bucket := range wantBuckets {
					ts := uint64(bucket.Ts.AsTime().UnixMilli())
					wantBucketsMap[ts] = bucket.DocCount
				}
				for _, bucket := range gotBuckets {
					ts := uint64(bucket.Ts.AsTime().UnixMilli())
					wantDocCount, ok := wantBucketsMap[ts]
					r.True(ok)
					r.Equal(wantDocCount, bucket.DocCount)
				}
			} else {
				r.Nil(got.Hist)
			}
		})
	}
}
