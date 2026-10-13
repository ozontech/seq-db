package proxyapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
)

var interval = "1m"

func TestBuildStreamSearchReqFromComplexSearchReq(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		req     *seqproxyapi.ComplexSearchRequest
		want    *search.StreamSearchRequest
		wantErr bool
	}{
		{
			name: "docs",
			req: &seqproxyapi.ComplexSearchRequest{
				Query: &seqproxyapi.SearchQuery{
					Query: "message:ok",
					From:  timestamppb.New(now),
					To:    timestamppb.New(now.Add(time.Hour)),
				},
				Size:      10,
				Offset:    5,
				Order:     seqproxyapi.Order_ORDER_ASC,
				WithTotal: true,
			},
			want: &search.StreamSearchRequest{
				Query:     "message:ok | sort asc | limit 10 | offset 5",
				From:      seq.TimeToMID(now),
				To:        seq.TimeToMID(now.Add(time.Hour)),
				Size:      10,
				Offset:    5,
				Order:     seq.DocsOrderAsc,
				WithTotal: true,
			},
		},
		{
			name: "docs_with_existing_fields_pipe",
			req: &seqproxyapi.ComplexSearchRequest{
				Query: &seqproxyapi.SearchQuery{
					Query: "message:ok | fields service, level",
					From:  timestamppb.New(now),
					To:    timestamppb.New(now.Add(time.Hour)),
				},
				Size:   10,
				Offset: 5,
				Order:  seqproxyapi.Order_ORDER_DESC,
			},
			want: &search.StreamSearchRequest{
				Query:  "message:ok | fields service, level | sort desc | limit 10 | offset 5",
				From:   seq.TimeToMID(now),
				To:     seq.TimeToMID(now.Add(time.Hour)),
				Size:   10,
				Offset: 5,
				Order:  seq.DocsOrderDesc,
			},
		},
		{
			name: "aggregation",
			req: &seqproxyapi.ComplexSearchRequest{
				Query: &seqproxyapi.SearchQuery{
					Query: "message:ok",
					From:  timestamppb.New(now),
					To:    timestamppb.New(now.Add(time.Hour)),
				},
				Aggs: []*seqproxyapi.AggQuery{{
					Field:   "duration",
					GroupBy: "service",
					Func:    seqproxyapi.AggFunc_AGG_FUNC_SUM,
				}},
			},
			want: &search.StreamSearchRequest{
				Query: "message:ok | stats sum(duration) by (service)",
				From:  seq.TimeToMID(now),
				To:    seq.TimeToMID(now.Add(time.Hour)),
				Agg: &search.AggQuery{
					Field:   "duration",
					GroupBy: "service",
					Func:    seq.AggFuncSum,
				},
			},
		},
		{
			name: "aggregation_with_interval",
			req: &seqproxyapi.ComplexSearchRequest{
				Query: &seqproxyapi.SearchQuery{
					Query: "message:ok",
					From:  timestamppb.New(now),
					To:    timestamppb.New(now.Add(time.Hour)),
				},
				Aggs: []*seqproxyapi.AggQuery{{
					Field:    "duration",
					GroupBy:  "service",
					Func:     seqproxyapi.AggFunc_AGG_FUNC_AVG,
					Interval: &interval,
				}},
			},
			want: &search.StreamSearchRequest{
				Query: "message:ok | stats avg(duration) by (service) interval(1m)",
				From:  seq.TimeToMID(now),
				To:    seq.TimeToMID(now.Add(time.Hour)),
				Agg: &search.AggQuery{
					Field:    "duration",
					GroupBy:  "service",
					Func:     seq.AggFuncAvg,
					Interval: seq.DurationToMID(1 * time.Minute),
				},
			},
		},
		{
			name: "invalid_query",
			req: &seqproxyapi.ComplexSearchRequest{
				Query: &seqproxyapi.SearchQuery{
					Query: "message:)",
					From:  timestamppb.New(now),
					To:    timestamppb.New(now.Add(time.Hour)),
				},
				Size: 10,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := buildStreamSearchReqFromComplexSearchReq(tc.req)
			require.Equal(t, tc.wantErr, err != nil)
			if tc.wantErr {
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
