package seqproxyapi

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAggregationBucketMarshalJSON(t *testing.T) {
	r := require.New(t)
	test := func(bucket *Aggregation_Bucket, expected string) {
		t.Helper()

		raw, err := json.Marshal(bucket)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &Aggregation_Bucket{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		// Handle math.NaN and math.Inf.
		if math.IsNaN(bucket.Value) || math.IsInf(bucket.Value, 0) {
			r.True(math.IsNaN(unmarshaled.Value) || math.IsInf(unmarshaled.Value, 0))
			bucket.Value = 0
			unmarshaled.Value = 0
		}
		r.Equal(bucket, unmarshaled)
	}

	test(&Aggregation_Bucket{}, `{"value":0}`)
	test(&Aggregation_Bucket{Value: 42}, `{"value":42}`)
	test(&Aggregation_Bucket{Value: math.NaN()}, `{"value":"NaN"}`)
	test(&Aggregation_Bucket{Value: math.Inf(1)}, `{"value":"+Inf"}`)
	test(&Aggregation_Bucket{Value: math.Inf(-1)}, `{"value":"-Inf"}`)
}

func TestStoreStatusValuesMarshalJSON(t *testing.T) {
	r := require.New(t)
	test := func(storeStatus *StoreStatusValues, expected string) {
		t.Helper()

		raw, err := json.Marshal(storeStatus)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &StoreStatusValues{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		r.Equal(storeStatus, unmarshaled)
	}

	test(&StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(999))}, `{"oldest_time":"1970-01-01T00:00:00.999Z"}`)
	test(&StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(9999999))}, `{"oldest_time":"1970-01-01T02:46:39.999Z"}`)
}

func TestStatusResponseMarshalJSON(t *testing.T) {
	r := require.New(t)
	test := func(status *StatusResponse, expected string) {
		t.Helper()

		raw, err := json.Marshal(status)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &StatusResponse{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		r.Equal(status, unmarshaled)
	}

	test(&StatusResponse{OldestStorageTime: nil}, `{"oldest_storage_time":null}`)
	test(&StatusResponse{OldestStorageTime: timestamppb.New(time.UnixMilli(999))}, `{"oldest_storage_time":"1970-01-01T00:00:00.999Z"}`)
	test(&StatusResponse{OldestStorageTime: timestamppb.New(time.UnixMilli(9999999))}, `{"oldest_storage_time":"1970-01-01T02:46:39.999Z"}`)
}

func TestExplainEntryMarshalJSON(t *testing.T) {
	r := require.New(t)

	test := func(explainEntry *ExplainEntry, expected string) {
		t.Helper()

		raw, err := json.Marshal(explainEntry)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &ExplainEntry{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		r.Equal(explainEntry, unmarshaled)
	}

	test(&ExplainEntry{Duration: durationpb.New(12 * time.Microsecond)}, `{"duration":"12µs"}`)
	test(&ExplainEntry{Duration: durationpb.New(8*time.Millisecond + 58*time.Microsecond)}, `{"duration":"8.058ms"}`)
	test(&ExplainEntry{Duration: durationpb.New(1*time.Second + 12*time.Millisecond)}, `{"duration":"1.012s"}`)
	test(&ExplainEntry{Duration: durationpb.New(2*time.Minute + 28*time.Second + 12*time.Millisecond)}, `{"duration":"2m28.012s"}`)
}

func TestFetchAsyncSearchResultResponseMarshalJSON(t *testing.T) {
	r := require.New(t)

	test := func(resp *FetchAsyncSearchResultResponse, expected string) {
		t.Helper()

		raw, err := json.Marshal(resp)
		r.NoError(err)
		r.Equal(expected, string(raw))
	}

	test(
		&FetchAsyncSearchResultResponse{
			Status: AsyncSearchStatus_AsyncSearchStatusCanceled,
			Request: &StartAsyncSearchRequest{
				Retention: durationpb.New(time.Duration(3600 * time.Second)),
				Query: &SearchQuery{
					Query: "message:some_message",
					From:  timestamppb.New(time.Date(2025, 7, 1, 5, 20, 0, 0, time.UTC)),
					To:    timestamppb.New(time.Date(2025, 8, 1, 5, 20, 0, 0, time.UTC)),
				},
				Aggs:     []*AggQuery{},
				Hist:     nil,
				WithDocs: true,
				Size:     100,
			},
			Response: &ComplexSearchResponse{
				Docs: []*Document{
					{
						Id:   "46e48be997010000-e70163d0fa7582e4",
						Data: []byte(`{"message":"some_message","level":3}`),
						Time: timestamppb.New(time.Date(2025, 7, 8, 10, 19, 8, 742000000, time.UTC)),
					},
				},
				Hist: &Histogram{},
			},
			StartedAt:  timestamppb.New(time.Date(2025, 7, 25, 12, 25, 57, 672000000, time.UTC)),
			ExpiresAt:  timestamppb.New(time.Date(2025, 7, 25, 13, 25, 57, 672000000, time.UTC)),
			CanceledAt: timestamppb.New(time.Date(2025, 7, 25, 12, 34, 26, 577000000, time.UTC)),
			Progress:   1,
			DiskUsage:  488,
		},
		`{"status":"AsyncSearchStatusCanceled","request":{"retention":"3600s","query":{"query":"message:some_message","from":"2025-07-01T05:20:00Z","to":"2025-08-01T05:20:00Z","explain":false},"aggs":[],"withDocs":true,"size":"100"},"response":{"docs":[{"id":"46e48be997010000-e70163d0fa7582e4","data":{"message":"some_message","level":3},"time":"2025-07-08T10:19:08.742Z"}],"hist":{}},"progress":1,"disk_usage":"488","started_at":"2025-07-25T12:25:57.672Z","expires_at":"2025-07-25T13:25:57.672Z","canceled_at":"2025-07-25T12:34:26.577Z"}`,
	)
}
