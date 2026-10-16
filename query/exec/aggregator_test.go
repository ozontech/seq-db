package exec

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

func TestDistributedAggregator_Count(t *testing.T) {
	input1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "key1", 0, 0, 0, 1, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "key2", 0, 0, 0, 1, 0, nil, nil),
	}}
	input2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "key1", 0, 0, 0, 1, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "key2", 0, 0, 0, 1, 0, nil, nil),
	}}
	agg := newTestAgg(seq.AggFuncCount, false, nil, input1, input2)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].AsString())
	assert.Equal(t, float64(2), results[0].Vals[1].AsFloat64())
	assert.Equal(t, "key2", results[1].Vals[0].AsString())
	assert.Equal(t, float64(2), results[1].Vals[1].AsFloat64())
}

func TestDistributedAggregator_Sum(t *testing.T) {
	input1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, false), "key1", 0, 0, 10, 1, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, false), "key2", 0, 0, 30, 1, 0, nil, nil),
	}}
	input2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, false), "key1", 0, 0, 20, 1, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, false), "key2", 0, 0, 40, 1, 0, nil, nil),
	}}
	agg := newTestAgg(seq.AggFuncSum, false, nil, input1, input2)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key2", results[0].Vals[0].AsString())
	assert.Equal(t, float64(70.0), results[0].Vals[1].AsFloat64())
	assert.Equal(t, "key1", results[1].Vals[0].AsString())
	assert.Equal(t, float64(30.0), results[1].Vals[1].AsFloat64())
}

func TestDistributedAggregator_Min(t *testing.T) {
	input1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "key1", 30, 0, 0, 0, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "key2", 20, 0, 0, 0, 0, nil, nil),
	}}
	input2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "key1", 10, 0, 0, 0, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "key2", 40, 0, 0, 0, 0, nil, nil),
	}}
	agg := newTestAgg(seq.AggFuncMin, false, nil, input1, input2)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].AsString())
	assert.Equal(t, float64(10.0), results[0].Vals[1].AsFloat64())
	assert.Equal(t, "key2", results[1].Vals[0].AsString())
	assert.Equal(t, float64(20.0), results[1].Vals[1].AsFloat64())
}

func TestDistributedAggregator_Max(t *testing.T) {
	input1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMax, false), "key1", 0, 10, 0, 0, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMax, false), "key2", 0, 30, 0, 0, 0, nil, nil),
	}}
	input2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMax, false), "key1", 0, 50, 0, 0, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMax, false), "key2", 0, 20, 0, 0, 0, nil, nil),
	}}
	agg := newTestAgg(seq.AggFuncMax, false, nil, input1, input2)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].AsString())
	assert.Equal(t, float64(50.0), results[0].Vals[1].AsFloat64())
	assert.Equal(t, "key2", results[1].Vals[0].AsString())
	assert.Equal(t, float64(30.0), results[1].Vals[1].AsFloat64())
}

func TestDistributedAggregator_Avg(t *testing.T) {
	input1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncAvg, false), "key1", 0, 0, 10, 1, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncAvg, false), "key2", 0, 0, 20, 1, 0, nil, nil),
	}}
	input2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncAvg, false), "key1", 0, 0, 30, 1, 0, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncAvg, false), "key2", 0, 0, 40, 1, 0, nil, nil),
	}}
	agg := newTestAgg(seq.AggFuncAvg, false, nil, input1, input2)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key2", results[0].Vals[0].AsString())
	assert.Equal(t, float64(30.0), results[0].Vals[1].AsFloat64())
	assert.Equal(t, "key1", results[1].Vals[0].AsString())
	assert.Equal(t, float64(20.0), results[1].Vals[1].AsFloat64())
}

func TestDistributedAggregator_EmptyInput(t *testing.T) {
	input := &testProducer{data: nil}
	agg := newTestAgg(seq.AggFuncCount, false, nil, input)

	results := collectRecords(agg)

	assert.Len(t, results, 0)
}

func TestDistributedAggregatorSumTimeseries(t *testing.T) {
	const ts1, ts2 uint64 = 1_000_000_000, 2_000_000_000
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, true), "foo", 0, 0, 2, 1, ts1, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, true), "foo", 0, 0, 3, 1, ts2, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, true), "bar", 0, 0, 5, 1, ts1, nil, nil),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, true), "foo", 0, 0, 4, 1, ts1, nil, nil), // same bin as in1[0] -> merged sum 6
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, true), "baz", 0, 0, 7, 1, ts2, nil, nil),
	}}

	a := newTestAgg(seq.AggFuncSum, true, nil, in1, in2)
	got := decodeAggOutputs(t, seq.AggFuncSum, true, a)

	// Sorted by ts ASC, then value DESC, then name ASC.
	want := []aggOutput{
		{"foo", 6, ts1, 0}, // 2 + 4
		{"bar", 5, ts1, 0},
		{"baz", 7, ts2, 0},
		{"foo", 3, ts2, 0},
	}
	assert.Equal(t, want, got)
}

func TestDistributedAggregatorNotExists(t *testing.T) {
	makeRec := func(token string, sum float64, notExists uint64) *query.Record {
		// sum input schema: [token, sum, total, not_exists]
		return query.NewRecord([]*query.RecordVals{
			query.NewStringVals(token),
			query.NewFloat64Vals(sum),
			query.NewUint64Vals(1),
			query.NewUint64Vals(notExists),
		})
	}

	in1 := &testProducer{data: []*query.Record{
		makeRec("a", 10, 1),
		makeRec("b", 1, 2),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeRec("a", 20, 3),
		makeRec("c", 9, 4),
	}}

	a := newTestAgg(seq.AggFuncSum, false, nil, in1, in2)
	got := decodeAggOutputs(t, seq.AggFuncSum, false, a)

	// "a": sum 10+20, not_exists 1+3=4; "b": 1/2; "c": 9/4.
	// Sorted by value DESC: a, c, b.
	want := []aggOutput{
		{"a", 30, 0, 4},
		{"c", 9, 0, 4},
		{"b", 1, 0, 2},
	}
	assert.Equal(t, want, got)
}

// TestDistributedAggregatorMinMergeAcrossShards checks that min/max are reduced
// across shards sharing a bin and that Min sorts ascending by value.
func TestDistributedAggregatorMinMergeAcrossShards(t *testing.T) {
	const ts uint64 = 0 // no interval: all samples collapse into ts=0
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "a", 5, 50, 0, 0, ts, nil, nil),
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "b", 1, 10, 0, 0, ts, nil, nil),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "a", 2, 60, 0, 0, ts, nil, nil), // min(5,2)=2, max(50,60)=60
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "c", 9, 9, 0, 0, ts, nil, nil),
	}}

	a := newTestAgg(seq.AggFuncMin, false, nil, in1, in2)
	got := decodeAggOutputs(t, seq.AggFuncMin, false, a)

	// Same ts (0); Min sorts by value ASC, then name ASC.
	want := []aggOutput{
		{"b", 1, ts, 0},
		{"a", 2, ts, 0},
		{"c", 9, ts, 0},
	}
	assert.Equal(t, want, got)
}

// TestDistributedAggregatorNoIntervalCollapsesByToken verifies that without an
// interval (ts=0 for every input) aggregation merges solely by token.
func TestDistributedAggregatorNoIntervalCollapsesByToken(t *testing.T) {
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "foo", 0, 0, 10, 2, 0, nil, nil),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "foo", 0, 0, 5, 3, 0, nil, nil),
	}}

	a := newTestAgg(seq.AggFuncCount, false, nil, in1, in2)
	got := decodeAggOutputs(t, seq.AggFuncCount, false, a)

	// count = total = 2 + 3 = 5. Output schema is [token, value] only.
	want := []aggOutput{
		{"foo", 5, 0, 0},
	}
	assert.Equal(t, want, got)
}

func TestDistributedAggregator_Quantile(t *testing.T) {
	const ts uint64 = 0
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncQuantile, false), "key", 1, 5, 0, 0, ts, []float64{1, 2, 3, 4, 5}, nil),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncQuantile, false), "key", 6, 10, 0, 0, ts, []float64{6, 7, 8, 9, 10}, nil),
	}}

	quantiles := []float64{0, 0.5, 0.9, 1}
	agg := newTestAgg(seq.AggFuncQuantile, false, quantiles, in1, in2)

	results := collectRecords(agg)
	assert.Len(t, results, 1)
	// quantile without interval: output schema is [token, value, quantiles, not_exists]
	out := query.AggResultSchemaFor(seq.AggFuncQuantile, false)
	assert.Equal(t, "key", out.MustColumn[string]("token").Val(results[0]))

	gotValue := out.MustColumn[float64]("value").Val(results[0])
	gotQuantiles := out.MustColumn[[]float64]("quantiles").Val(results[0])
	assert.Len(t, gotQuantiles, len(quantiles))
	assert.Equal(t, gotQuantiles[0], gotValue)

	assert.InDelta(t, 1, gotQuantiles[0], 0.5)
	assert.InDelta(t, 5.5, gotQuantiles[1], 0.5)
	assert.InDelta(t, 9.1, gotQuantiles[2], 0.5)
	assert.InDelta(t, 10, gotQuantiles[3], 0.5)
}

func TestDistributedAggregator_UniqueCount(t *testing.T) {
	in1 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncUniqueCount, false), "key", 0, 0, 0, 0, 0, nil, []string{"a", "b", "c"}),
	}}
	in2 := &testProducer{data: []*query.Record{
		makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncUniqueCount, false), "key", 0, 0, 0, 0, 0, nil, []string{"b", "c", "d"}),
	}}
	agg := newTestAgg(seq.AggFuncUniqueCount, false, nil, in1, in2)

	results := collectRecords(agg)
	assert.Len(t, results, 1)
	assert.Equal(t, "key", results[0].Vals[0].AsString())
	assert.Equal(t, float64(4), results[0].Vals[1].AsFloat64())
}

func TestDistributedAggregatorEmptyShard(t *testing.T) {
	const ts uint64 = 0

	empty := func(fn seq.AggFunc) *query.Record {
		return makeAggInputRecord(query.AggsSchemaFor(fn, false), "k", math.NaN(), math.NaN(), 0, 0, ts, nil, nil)
	}

	tests := []struct {
		name     string
		fn       seq.AggFunc
		quantile []float64
		real     *query.Record
		want     float64
	}{
		{
			name: "min ignores empty shard",
			fn:   seq.AggFuncMin,
			real: makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMin, false), "k", 7, 0, 0, 0, ts, nil, nil),
			want: 7,
		},
		{
			name: "max ignores empty shard",
			fn:   seq.AggFuncMax,
			real: makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncMax, false), "k", 0, 7, 0, 0, ts, nil, nil),
			want: 7,
		},
		{
			name: "sum ignores empty shard",
			fn:   seq.AggFuncSum,
			real: makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncSum, false), "k", 0, 0, 7, 2, ts, nil, nil),
			want: 7,
		},
		{
			name: "avg ignores empty shard",
			fn:   seq.AggFuncAvg,
			real: makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncAvg, false), "k", 0, 0, 7, 2, ts, nil, nil),
			want: 3.5,
		},
		{
			name:     "quantile ignores empty shard",
			fn:       seq.AggFuncQuantile,
			quantile: []float64{0, 1},
			real:     makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncQuantile, false), "k", 3, 9, 0, 0, ts, []float64{3, 5, 9}, nil),
			want:     3, // q=0 -> min
		},
		{
			name: "unique_count ignores empty shard",
			fn:   seq.AggFuncUniqueCount,
			real: makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncUniqueCount, false), "k", 0, 0, 0, 0, ts, nil, []string{"a", "b"}),
			want: 2,
		},
		{
			name: "count counts empty shard as zero",
			fn:   seq.AggFuncCount,
			real: makeAggInputRecord(query.AggsSchemaFor(seq.AggFuncCount, false), "k", 0, 0, 0, 2, ts, nil, nil),
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := newTestAgg(tt.fn, false, tt.quantile,
				&testProducer{data: []*query.Record{empty(tt.fn)}},
				&testProducer{data: []*query.Record{tt.real}})

			got := decodeAggOutputs(t, tt.fn, false, agg)
			assert.Len(t, got, 1)
			assert.Equal(t, tt.want, got[0].value)
		})
	}
}

func TestDistributedAggregatorEmptyBucketNaN(t *testing.T) {
	for _, tt := range []struct {
		name     string
		fn       seq.AggFunc
		quantile []float64
		empty    bool
	}{
		{name: "min", fn: seq.AggFuncMin, empty: true},
		{name: "max", fn: seq.AggFuncMax, empty: true},
		{name: "sum", fn: seq.AggFuncSum, empty: true},
		{name: "avg", fn: seq.AggFuncAvg, empty: true},
		{name: "quantile", fn: seq.AggFuncQuantile, quantile: []float64{0.5}, empty: true},
		{name: "unique_count", fn: seq.AggFuncUniqueCount, empty: true},
		{name: "count stays zero", fn: seq.AggFuncCount},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := makeAggInputRecord(query.AggsSchemaFor(tt.fn, false), "k", math.NaN(), math.NaN(), 0, 0, 0, nil, nil)
			agg := newTestAgg(tt.fn, false, tt.quantile, &testProducer{data: []*query.Record{rec}})

			got := decodeAggOutputs(t, tt.fn, false, agg)
			assert.Len(t, got, 1)
			if tt.empty {
				assert.True(t, math.IsNaN(got[0].value), "want NaN, got %v", got[0].value)
			} else {
				assert.Equal(t, float64(0), got[0].value)
			}
		})
	}
}

func TestDistributedAggregatorFinalize(t *testing.T) {
	in1 := &testProducer{data: nil, total: 40, notExists: 2}
	in2 := &testProducer{data: nil, total: 60, notExists: 3}
	agg := newTestAgg(seq.AggFuncCount, false, nil, in1, in2)
	for r := agg.Next(); r != nil; r = agg.Next() {
		t.Fatalf("unexpected record: %v", r)
	}
	summary := agg.Finalize()
	assert.NotNil(t, summary)
	assert.Equal(t, uint64(100), summary.Total)
	assert.Equal(t, uint64(5), summary.NotExists)
	assert.Nil(t, summary.Err)
}

func newTestAgg(fn seq.AggFunc, timeseries bool, quantiles []float64, inputs ...query.RecordProducer) *DistributedAggregator {
	return NewDistributedAggregator(inputs, query.AggsSchemaFor(fn, timeseries), fn, quantiles)
}

func makeAggInputRecord(
	schema *query.Schema,
	token string,
	mn, mx, sum float64,
	total, ts uint64,
	samples []float64,
	values []string,
) *query.Record {
	vals := make([]*query.RecordVals, 0, schema.Len())
	for _, col := range schema.Cols() {
		var v *query.RecordVals
		switch col.Name {
		case "token":
			v = query.NewStringVals(token)
		case "ts":
			v = query.NewUint64Vals(ts)
		case "min":
			v = query.NewFloat64Vals(mn)
		case "max":
			v = query.NewFloat64Vals(mx)
		case "sum":
			v = query.NewFloat64Vals(sum)
		case "total":
			v = query.NewUint64Vals(total)
		case "not_exists":
			v = query.NewUint64Vals(0) // unused by the aggregator
		case "samples":
			v = query.NewFloat64ArrayVals(samples)
		case "values":
			v = query.NewStringArrayVals(values)
		default:
			panic("unknown aggregation column " + col.Name)
		}
		vals = append(vals, v)
	}
	return query.NewRecord(vals)
}

// nolint:unused // fields are checked but not directly
type aggOutput struct {
	key       string
	value     float64
	ts        uint64
	notExists uint64
}

func decodeAggOutputs(t *testing.T, fn seq.AggFunc, timeseries bool, a *DistributedAggregator) []aggOutput {
	t.Helper()
	out := query.AggResultSchemaFor(fn, timeseries)
	token := out.MustColumn[string]("token")
	value := out.MustColumn[float64]("value")
	tsCol := query.OptionalColumn[uint64](out, "ts")
	neCol := query.OptionalColumn[uint64](out, "not_exists")

	decoded := make([]aggOutput, 0)
	for r := a.Next(); r != nil; r = a.Next() {
		var ts, notExists uint64
		if tsCol != nil {
			ts = tsCol.Val(r)
		}
		if neCol != nil {
			notExists = neCol.Val(r)
		}
		decoded = append(decoded, aggOutput{
			key:       token.Val(r),
			value:     value.Val(r),
			ts:        ts,
			notExists: notExists,
		})
	}
	return decoded
}
