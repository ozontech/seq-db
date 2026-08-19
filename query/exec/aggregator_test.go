package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
	"github.com/ozontech/seq-db/seq"
)

func TestDistributedAggregator_Count(t *testing.T) {
	records1 := []*query.Record{
		makeAggInputRecord("key1", 10.0, 10.0, 10.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 20.0, 20.0, 20.0, 1, 0, []float64{}),
	}

	records2 := []*query.Record{
		makeAggInputRecord("key1", 30.0, 30.0, 30.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 40.0, 40.0, 40.0, 1, 0, []float64{}),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncCount, nil)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(2), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key2", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(2), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Sum(t *testing.T) {
	records1 := []*query.Record{
		makeAggInputRecord("key1", 10.0, 10.0, 10.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 30.0, 30.0, 30.0, 1, 0, []float64{}),
	}

	records2 := []*query.Record{
		makeAggInputRecord("key1", 20.0, 20.0, 20.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 40.0, 40.0, 40.0, 1, 0, []float64{}),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncSum, nil)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key2", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(70.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key1", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(30.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Min(t *testing.T) {
	records1 := []*query.Record{
		makeAggInputRecord("key1", 30.0, 30.0, 30.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 20.0, 20.0, 20.0, 1, 0, []float64{}),
	}

	records2 := []*query.Record{
		makeAggInputRecord("key1", 10.0, 10.0, 10.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 40.0, 40.0, 40.0, 1, 0, []float64{}),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncMin, nil)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(10.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key2", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(20.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Max(t *testing.T) {
	records1 := []*query.Record{
		makeAggInputRecord("key1", 10.0, 10.0, 10.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 30.0, 30.0, 30.0, 1, 0, []float64{}),
	}

	records2 := []*query.Record{
		makeAggInputRecord("key1", 50.0, 50.0, 50.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 20.0, 20.0, 20.0, 1, 0, []float64{}),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncMax, nil)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(50.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key2", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(30.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Avg(t *testing.T) {
	records1 := []*query.Record{
		makeAggInputRecord("key1", 10.0, 10.0, 10.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 20.0, 20.0, 20.0, 1, 0, []float64{}),
	}

	records2 := []*query.Record{
		makeAggInputRecord("key1", 30.0, 30.0, 30.0, 1, 0, []float64{}),
		makeAggInputRecord("key2", 40.0, 40.0, 40.0, 1, 0, []float64{}),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncAvg, nil)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key2", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(30.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key1", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(20.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_EmptyInput(t *testing.T) {
	var records []*query.Record

	input := testProducer{data: records}
	agg := NewDistributedAggregator([]query.RecordProducer{&input}, seq.AggFuncCount, nil)

	results := collectRecords(agg)

	assert.Len(t, results, 0)
}

func TestDistributedAggregatorSumTimeseries(t *testing.T) {
	const ts1, ts2 uint64 = 1_000_000_000, 2_000_000_000
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord("foo", 2, 2, 2, 1, ts1, []float64{}),
		makeAggInputRecord("foo", 3, 3, 3, 1, ts2, []float64{}),
		makeAggInputRecord("bar", 5, 5, 5, 1, ts1, []float64{}),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord("foo", 4, 4, 4, 1, ts1, []float64{}), // same bin as in1[0] -> merged sum 6
		makeAggInputRecord("baz", 7, 7, 7, 1, ts2, []float64{}),
	}}

	a := NewDistributedAggregator([]query.RecordProducer{in1, in2}, seq.AggFuncSum, nil)
	got := decodeAggOutputs(t, a)

	// Sorted by ts ASC, then value DESC, then name ASC.
	want := []aggOutput{
		{"foo", 6, ts1}, // 2 + 4
		{"bar", 5, ts1},
		{"baz", 7, ts2},
		{"foo", 3, ts2},
	}
	assert.Equal(t, want, got)
}

// TestDistributedAggregatorMinMergeAcrossShards checks that min/max are reduced
// across shards sharing a bin and that Min sorts ascending by value.
func TestDistributedAggregatorMinMergeAcrossShards(t *testing.T) {
	const ts uint64 = 0 // no interval: all samples collapse into ts=0
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord("a", 5, 50, 0, 1, ts, []float64{}),
		makeAggInputRecord("b", 1, 10, 0, 1, ts, []float64{}),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord("a", 2, 60, 0, 1, ts, []float64{}), // min(5,2)=2, max(50,60)=60
		makeAggInputRecord("c", 9, 9, 0, 1, ts, []float64{}),
	}}

	a := NewDistributedAggregator([]query.RecordProducer{in1, in2}, seq.AggFuncMin, nil)
	got := decodeAggOutputs(t, a)

	// Same ts (0); Min sorts by value ASC, then name ASC.
	want := []aggOutput{
		{"b", 1, ts},
		{"a", 2, ts},
		{"c", 9, ts},
	}
	assert.Equal(t, want, got)
}

// TestDistributedAggregatorNoIntervalCollapsesByToken verifies that without an
// interval (ts=0 for every input) aggregation merges solely by token.
func TestDistributedAggregatorNoIntervalCollapsesByToken(t *testing.T) {
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord("foo", 0, 0, 10, 2, 0, []float64{}),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord("foo", 0, 0, 5, 3, 0, []float64{}),
	}}

	a := NewDistributedAggregator([]query.RecordProducer{in1, in2}, seq.AggFuncCount, nil)
	got := decodeAggOutputs(t, a)

	// count = total = 2 + 3 = 5.
	want := []aggOutput{
		{"foo", 5, 0},
	}
	assert.Equal(t, want, got)
}

func TestDistributedAggregator_Quantile(t *testing.T) {
	const ts uint64 = 0
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord("key", 1, 5, 0, 0, ts, []float64{1, 2, 3, 4, 5}),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord("key", 6, 10, 0, 0, ts, []float64{6, 7, 8, 9, 10}),
	}}

	quantiles := []float64{0, 0.5, 0.9, 1}
	agg := NewDistributedAggregator([]query.RecordProducer{in1, in2}, seq.AggFuncQuantile, quantiles)

	results := collectRecords(agg)
	assert.Len(t, results, 1)
	assert.Equal(t, "key", results[0].Vals[0].Decoded().(string))

	gotValue := results[0].Vals[1].Decoded().(float64)
	gotQuantiles := results[0].Vals[3].Decoded().([]float64)
	assert.Len(t, gotQuantiles, len(quantiles))
	assert.Equal(t, gotQuantiles[0], gotValue)

	assert.InDelta(t, 1, gotQuantiles[0], 0.5)
	assert.InDelta(t, 5.5, gotQuantiles[1], 0.5)
	assert.InDelta(t, 9.1, gotQuantiles[2], 0.5)
	assert.InDelta(t, 10, gotQuantiles[3], 0.5)
}

func TestDistributedAggregatorFinalize(t *testing.T) {
	in1 := &testProducer{data: nil, total: 40}
	in2 := &testProducer{data: nil, total: 60}
	a := NewDistributedAggregator([]query.RecordProducer{in1, in2}, seq.AggFuncCount, nil)
	for r := a.Next(); r != nil; r = a.Next() {
		t.Fatalf("unexpected record: %v", r)
	}
	summary := a.Finalize()
	assert.NotNil(t, summary)
	assert.Equal(t, uint64(100), summary.Total)
	assert.Nil(t, summary.Err)
}

func makeAggInputRecord(token string, mn, mx, sum float64, total, ts uint64, samples []float64) *query.Record {
	return query.NewRecord([]*query.RecordVals{
		query.NewRecordVals(query.DataTypeString, []byte(token)),
		query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(mn)),
		query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(mx)),
		query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(sum)),
		query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(total)),
		query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(0)), // not_exists, unused by aggregator
		query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(ts)),
		query.NewRecordVals(query.DataTypeFloat64Array, encoding.Float64ArrayToBytes(samples)),
	})
}

// nolint:unused // fields are checked but not directly
type aggOutput struct {
	key   string
	value float64
	ts    uint64
}

func decodeAggOutputs(t *testing.T, a *DistributedAggregator) []aggOutput {
	t.Helper()
	out := make([]aggOutput, 0)
	for r := a.Next(); r != nil; r = a.Next() {
		out = append(out, aggOutput{
			key:   r.Vals[0].Decoded().(string),
			value: r.Vals[1].Decoded().(float64),
			ts:    r.Vals[2].Decoded().(uint64),
		})
	}
	return out
}
