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

type getHistogramTestCaseData struct {
	searchQ   *testSearchQuery
	histQ     *testHistQuery
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

type getHistogramTestData struct {
	req   *seqproxyapi.GetHistogramRequest
	want  *seqproxyapi.GetHistogramResponse
	mData *mocksData
}

func checkGetHistQueries(t *testing.T, cData getHistogramTestCaseData) {
	if cData.searchQ == nil {
		t.Fatal("search query must be provided")
	}
	if cData.histQ == nil {
		t.Fatal("hist query must be provided")
	}
}

func prepareGetHistogramTestData(t *testing.T, cData getHistogramTestCaseData) getHistogramTestData {
	req := &seqproxyapi.GetHistogramRequest{}
	if cData.searchQ != nil {
		req.Query = &seqproxyapi.SearchQuery{
			Query:   cData.searchQ.query,
			From:    &timestamppb.Timestamp{Seconds: cData.searchQ.from.Unix()},
			To:      &timestamppb.Timestamp{Seconds: cData.searchQ.to.Unix() + 1},
			Explain: cData.searchQ.explain,
		}
	}
	if cData.histQ != nil {
		req.Hist = &seqproxyapi.HistQuery{
			Interval: cData.histQ.interval,
		}
	}

	var resp *seqproxyapi.GetHistogramResponse
	qpr := &seq.QPR{Errors: cData.qprErr}
	if !cData.noResp {
		checkGetHistQueries(t, cData)
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
		qpr.Total = uint64(cData.totalSize)
		resp = &seqproxyapi.GetHistogramResponse{
			Total:           cData.totalSize,
			Hist:            hRespData.apiHist,
			PartialResponse: cData.partial,
			Error:           cData.respErr,
		}
	}

	var siSearchMock *siSearchMockData
	if !cData.noSiMock {
		checkGetHistQueries(t, cData)
		intervalDur, err := util.ParseDuration(cData.histQ.interval)
		if err != nil {
			t.Fatalf("failed to parse interval duration: %q", cData.histQ.interval)
		}
		sr := &search.SearchRequest{
			Explain:  req.Query.Explain,
			Q:        []byte(req.Query.Query),
			From:     seq.MID(req.Query.From.AsTime().UnixMilli()),
			To:       seq.MID(req.Query.To.AsTime().UnixMilli()),
			Interval: seq.DurationToMID(intervalDur),
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
		checkGetHistQueries(t, cData)
		rlQuery := []string{req.Query.Query, req.Hist.Interval}
		rlMock = &rlMockData{
			query:   strings.Join(rlQuery, ","),
			limited: cData.rateLimited,
		}
	}

	return getHistogramTestData{
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

func TestGrpcV1_GetHistogram(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		data    getHistogramTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ok",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "10m",
				},
				totalSize: 1000,
				respErr: &seqproxyapi.Error{
					Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
				},
			},
			wantErr: false,
		},
		{
			name: "partial_resp",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:partial",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "10m",
				},
				totalSize: 1000,
				partial:   true,
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
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ingestor-err",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "10m",
				},
				noResp: true,
				siErr:  errors.New("test"),
			},
			wantErr: true,
		},
		{
			name: "qpr_err",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:qpr-err",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "10m",
				},
				noResp: true,
				qprErr: []seq.ErrorSource{
					{ErrStr: "test err", Source: 0},
				},
			},
			wantErr: true,
		},
		{
			name: "old_data_err",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:old-data-err",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "10m",
				},
				noResp: true,
				siErr:  consts.ErrIngestorQueryWantsOldData,
			},
			wantErr: true,
		},
		{
			name: "invalid_interval_err",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:interval-err",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "error",
				},
				noResp:   true,
				noSiMock: true,
			},
			wantErr: true,
		},
		{
			name: "rate_limited_err",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:ratelimited",
					from:  now,
					to:    now.Add(time.Hour),
				},
				histQ: &testHistQuery{
					interval: "10m",
				},
				noResp:      true,
				noSiMock:    true,
				rateLimited: true,
			},
			wantErr: true,
		},
		{
			name: "nil_search_query_err",
			data: getHistogramTestCaseData{
				histQ: &testHistQuery{
					interval: "10m",
				},
				noResp:   true,
				noSiMock: true,
				noRlMock: true,
			},
			wantErr: true,
		},
		{
			name: "nil_agg_query_err",
			data: getHistogramTestCaseData{
				searchQ: &testSearchQuery{
					query: "message:nil-hist-q",
					from:  now,
					to:    now.Add(time.Hour),
				},
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

			testData := prepareGetHistogramTestData(t, tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			got, err := a.s.GetHistogram(a.ctx, testData.req)
			r.Equal(tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			r.NotNil(got)
			r.Equal(testData.want.PartialResponse, got.PartialResponse)
			r.Equal(testData.want.Error, got.Error)
			r.Equal(testData.want.Total, got.Total)
			if testData.want.Hist == nil {
				r.NotNil(got.Hist)
				return
			}
			wantBuckets := testData.want.Hist.Buckets
			gotBuckets := got.Hist.Buckets
			r.Equal(len(wantBuckets), len(gotBuckets))
			wantBucketsMap := make(map[uint64]uint64, len(wantBuckets))
			for _, bucket := range wantBuckets {
				ts := uint64(bucket.Ts.AsTime().Local().UnixMilli())
				wantBucketsMap[ts] = bucket.DocCount
			}
			for _, bucket := range gotBuckets {
				ts := uint64(bucket.Ts.AsTime().Local().UnixMilli())
				wantDocCount, ok := wantBucketsMap[ts]
				r.True(ok)
				r.Equal(wantDocCount, bucket.DocCount)
			}
		})
	}
}
