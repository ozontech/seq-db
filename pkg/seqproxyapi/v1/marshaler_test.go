package seqproxyapi

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
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
