package asyncsearcher

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
)

func TestQPRMarshalUnmarshal(t *testing.T) {
	test := func(qpr seq.QPR) {
		t.Helper()

		rawQPR := marshalQPR(&qpr, nil)
		var out seq.QPR
		tail, err := unmarshalQPR(&out, rawQPR, math.MaxInt)
		require.NoError(t, err)
		require.Equal(t, 0, len(tail))
		qpr.ToCompressed()
		require.EqualExportedValues(t, qpr, out)
		require.Equal(t, qprBinVersion3, qprBinVersion(rawQPR[0]))
	}

	test(seq.QPR{Histogram: map[seq.MID]uint64{}})
	test(seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Errors:    []seq.ErrorSource{{ErrStr: "error", Source: 1}},
	})
	test(seq.QPR{
		IDs: seq.IDSources{
			{
				ID: seq.ID{MID: 42, RID: 13},
			},
		},
		Histogram: map[seq.MID]uint64{42: 1},
		Total:     1,
	})

	test(seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{testAggSamples()},
	})

	test(seq.QPR{
		Histogram: map[seq.MID]uint64{},
		IDs: seq.IDSources{
			seq.IDSource{ID: seq.ID{MID: 42, RID: 13}},
		},
		Total:  545454,
		Errors: []seq.ErrorSource{{ErrStr: "context canceled", Source: 8956}},
	})

	for i := 0; i < 100; i++ {
		r := rand.N(8)
		qpr := getRandomQPR(r * 1024)
		test(qpr)
	}
}

func testAggSamples() seq.AggregatableSamples {
	agg := seq.AggregatableSamplesFromBins([]seq.AggBinSamples{
		{Bin: seq.AggBin{Token: "_not_exists"}, Samples: seq.SamplesContainer{Total: 1}},
		{
			Bin: seq.AggBin{Token: "seq-db proxy"},
			Samples: seq.SamplesContainer{
				Min:     0,
				Max:     100,
				Sum:     100,
				Total:   1,
				Samples: []float64{100},
			},
		},
		{
			Bin: seq.AggBin{Token: "seq-db store", MID: seq.MID(2)},
			Samples: seq.SamplesContainer{
				Min:       3,
				Max:       5,
				Sum:       794,
				Total:     1,
				NotExists: 7,
				Samples:   []float64{324},
			},
		},
		{
			Bin: seq.AggBin{Token: "seq-db store", MID: seq.MID(1)},
			Samples: seq.SamplesContainer{
				Min:       2,
				Max:       6,
				Sum:       544,
				Total:     2,
				NotExists: 3,
				Samples:   []float64{324},
			},
		},
	})
	agg.SortRows()
	agg.NotExists = 5412
	agg.ValuesPool = []string{"error", "info"}
	return agg
}

// TestQPRVersion1Compatibility tests that it's possible to unmarshall and read version 1 async search result encoded.
// MIDs in IDs and a histogram must be converted from millis to nanos
func TestQPRVersion1Compatibility(t *testing.T) {
	qpr := seq.QPR{
		IDs: seq.IDSources{
			{
				ID: seq.ID{MID: seq.MID(1761812502573), RID: 34734732392},
			},
		},
		Histogram: map[seq.MID]uint64{
			seq.MID(1761812502573): 433,
			seq.MID(1761812502463): 743,
		},
		Aggs: []seq.AggregatableSamples{{
			Tokens: []seq.StringColumn{stringColumn("_not_exists", "seq-db store", "seq-db store")},
			MIDs: midsColumn(
				consts.DummyMID,
				seq.MID(1761812502456),
				seq.MID(1761812502953),
			),
			Values: seq.NewAggValuesFromSamples(seq.AggFuncQuantile, []seq.SamplesContainer{
				{Total: 1},
				{Min: 2, Max: 6, Sum: 544, Total: 2, NotExists: 3, Samples: []float64{324}},
				{Min: 3, Max: 5, Sum: 794, Total: 1, NotExists: 7, Samples: []float64{324}},
			}),
			NotExists: 5412,
		}},
	}

	rawQPR := marshalQPRVersion(&qpr, nil, qprBinVersion1)

	var outQPR seq.QPR
	tail, err := unmarshalQPR(&outQPR, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Equal(t, seq.MID(1761812502573000000), outQPR.IDs[0].ID.MID)

	require.Len(t, outQPR.Histogram, 2)
	require.Equal(t, uint64(433), outQPR.Histogram[seq.MID(1761812502573000000)])
	require.Equal(t, uint64(743), outQPR.Histogram[seq.MID(1761812502463000000)])

	require.Len(t, outQPR.Aggs, 1)
	agg := outQPR.Aggs[0]
	require.Equal(t, 3, agg.MIDs.Len())
	require.Equal(t, int64(1), agg.FindBin(seq.AggBin{Token: "_not_exists"}).Total)
	require.Equal(t, int64(1), agg.FindBin(seq.AggBin{Token: "seq-db store", MID: seq.MID(1761812502953000000)}).Total)
	require.Equal(t, int64(2), agg.FindBin(seq.AggBin{Token: "seq-db store", MID: seq.MID(1761812502456000000)}).Total)
}

func TestQPRSortsAggBins(t *testing.T) {
	sortedAgg := seq.AggregatableSamplesFromBins([]seq.AggBinSamples{
		{Bin: seq.AggBin{Token: "a", MID: 3}},
		{Bin: seq.AggBin{Token: "b", MID: 2}},
		{Bin: seq.AggBin{Token: "a", MID: 1}},
	})
	sortedQPR := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{sortedAgg},
	}

	rawQPR := marshalQPR(&sortedQPR, nil)
	var version3 seq.QPR
	tail, err := unmarshalQPR(&version3, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	requireAggsEqual(t, sortedQPR.Aggs[0], version3.Aggs[0])

	unsortedQPR := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs: []seq.AggregatableSamples{{
			Tokens: []seq.StringColumn{stringColumn("b", "a", "a")},
			MIDs:   midsColumn(2, 3, 1),
			Values: seq.NewAggValuesFromSamples(seq.AggFuncCount, []seq.SamplesContainer{
				{}, {}, {},
			}),
		}},
	}
	rawQPR = slices.Clone(marshalQPRVersion(&unsortedQPR, nil, qprBinVersion2))

	var version2 seq.QPR
	tail, err = unmarshalQPR(&version2, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	requireAggsEqual(t, sortedAgg, version2.Aggs[0])

	rawQPR = slices.Clone(marshalQPRVersion(&unsortedQPR, nil, qprBinVersion3))

	var version3Unsorted seq.QPR
	_, err = unmarshalQPR(&version3Unsorted, rawQPR, math.MaxInt)
	require.ErrorContains(t, err, "unsorted aggregation columns")
}

func TestQPRVersion3ColumnarAggregations(t *testing.T) {
	for _, fn := range []seq.AggFunc{
		seq.AggFuncCount,
		seq.AggFuncSum,
		seq.AggFuncMin,
		seq.AggFuncMax,
		seq.AggFuncAvg,
		seq.AggFuncQuantile,
		seq.AggFuncUnique,
		seq.AggFuncUniqueCount,
	} {
		t.Run(fn.String(), func(t *testing.T) {
			samples := []seq.SamplesContainer{
				{
					Min:       1,
					Max:       3,
					Sum:       4,
					Total:     2,
					NotExists: 5,
					Samples:   []float64{1, 3},
					Values:    map[uint32]struct{}{1: {}, 2: {}},
				},
				{
					Min:       2,
					Max:       4,
					Sum:       6,
					Total:     1,
					NotExists: 6,
					Samples:   []float64{2, 4},
					Values:    map[uint32]struct{}{2: {}, 3: {}},
				},
			}
			expected := seq.NewAggregatableSamplesForFunc(fn, 1, len(samples))
			expected.AppendRow([]string{"a"}, 2, samples[0])
			expected.AppendRow([]string{"b"}, 1, samples[1])
			expected.SortRows()

			qpr := seq.QPR{
				Histogram: map[seq.MID]uint64{},
				Aggs:      []seq.AggregatableSamples{*expected},
			}
			rawQPR := marshalQPR(&qpr, nil)
			require.Equal(t, qprBinVersion3, qprBinVersion(rawQPR[0]))

			var actual seq.QPR
			tail, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
			require.NoError(t, err)
			require.Equal(t, 0, len(tail))
			require.Len(t, actual.Aggs, 1)
			requireAggsEqual(t, *expected, actual.Aggs[0])
		})
	}
}

func TestQPRVersion3Compatibility(t *testing.T) {
	expected := testAggSamples()
	qpr := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{expected},
	}

	rawQPR := marshalQPRVersion(&qpr, nil, qprBinVersion3)
	var actual seq.QPR
	tail, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Len(t, actual.Aggs, 1)
	requireAggsEqual(t, expected, actual.Aggs[0])
}

func TestQPRVersion3MultipleTokenColumns(t *testing.T) {
	expected := seq.NewAggregatableSamplesForFunc(seq.AggFuncCount, 2, 2)
	expected.AppendRow([]string{"a", "z"}, 2, seq.SamplesContainer{Total: 1})
	expected.AppendRow([]string{"b", "a"}, 1, seq.SamplesContainer{Total: 2})
	expected.SortRows()
	qpr := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{*expected},
	}

	rawQPR := marshalQPR(&qpr, nil)
	var actual seq.QPR
	tail, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Len(t, actual.Aggs, 1)
	requireAggsEqual(t, *expected, actual.Aggs[0])
}

func TestQPRVersion3RejectsUnsortedAggregation(t *testing.T) {
	agg := seq.NewAggregatableSamplesForFunc(seq.AggFuncCount, 1, 2)
	agg.AppendRow([]string{"b"}, 2, seq.SamplesContainer{Total: 1})
	agg.AppendRow([]string{"a"}, 2, seq.SamplesContainer{Total: 1})
	qpr := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{*agg},
	}

	rawQPR := marshalQPR(&qpr, nil)
	var actual seq.QPR
	_, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
	require.ErrorContains(t, err, "unsorted aggregation columns")
}

func requireAggsEqual(t *testing.T, expected, actual seq.AggregatableSamples) {
	t.Helper()
	require.Equal(t, expected.RowsCount(), actual.RowsCount())
	require.Equal(t, expected.Values.Func, actual.Values.Func)
	require.Equal(t, expected.NotExists, actual.NotExists)
	require.Equal(t, expected.ValuesPool, actual.ValuesPool)

	iters := make([]seq.StringColumnIter, len(expected.Tokens))
	expectedIters := make([]seq.StringColumnIter, len(expected.Tokens))
	var actualMIDs seq.Int64ColumnIter
	var actualTotals seq.Int64ColumnIter
	var actualNotExists seq.Int64ColumnIter
	for i := range iters {
		iters[i] = actual.Tokens[i].NewIter()
		expectedIters[i] = expected.Tokens[i].NewIter()
	}
	if actual.MIDs != nil {
		actualMIDs = actual.MIDs.NewIter()
	}
	actualTotals = actual.Values.Totals.NewIter()
	actualNotExists = actual.Values.NotExists.NewIter()
	for row := range expected.RowsCount() {
		for i := range iters {
			require.True(t, iters[i].Next())
			require.True(t, expectedIters[i].Next())
			require.Equal(t, string(expectedIters[i].Row()), string(iters[i].Row()))
		}
		actualMID := seq.MID(consts.DummyMID)
		if actualMIDs != nil && actualMIDs.Next() {
			actualMID = seq.MID(actualMIDs.Value())
		}
		require.Equal(t, expected.RowMID(row), actualMID)
		require.Equal(t, expected.RowSamples(row), actualRowSamples(&actual, row, actualTotals, actualNotExists))
	}
}

func actualRowSamples(
	agg *seq.AggregatableSamples,
	row int,
	totals, notExists seq.Int64ColumnIter,
) *seq.SamplesContainer {
	samples := &seq.SamplesContainer{}
	switch agg.Values.Func {
	case seq.AggFuncCount, seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg, seq.AggFuncQuantile, seq.AggFuncUniqueCount:
		if totals.Next() {
			samples.Total = totals.Value()
		}
	}
	switch agg.Values.Func {
	case seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg, seq.AggFuncQuantile, seq.AggFuncUniqueCount:
		if notExists.Next() {
			samples.NotExists = notExists.Value()
		}
	}
	switch agg.Values.Func {
	case seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg, seq.AggFuncQuantile:
		samples.Sum = agg.Values.Sums.At(row)
		samples.Min = agg.Values.Mins.At(row)
		samples.Max = agg.Values.Maxs.At(row)
	}
	if agg.Values.Func == seq.AggFuncQuantile {
		samples.Samples = agg.Values.Quantiles.Row(row)
	}
	if agg.Values.Func == seq.AggFuncUniqueCount {
		samples.Values = make(map[uint32]struct{}, len(agg.Values.UniqueIDs.Row(row)))
		for _, id := range agg.Values.UniqueIDs.Row(row) {
			samples.Values[id] = struct{}{}
		}
	}
	return samples
}

func getRandomQPR(size int) seq.QPR {
	curTime := time.Now()
	getTime := func() time.Time {
		curTime = curTime.Add(500 * time.Microsecond)
		return curTime
	}

	var ids seq.IDSources
	for i := 0; i < size; i++ {
		mid := getTime()
		rid := rand.N[uint64](math.MaxUint64)
		src := rand.N[uint64](math.MaxUint64)
		ids = append(ids, seq.IDSource{ID: seq.NewID(mid, rid), Source: src})
	}

	var aggs []seq.AggregatableSamples
	for i := 0; i < size; i++ {
		hist := aggSamplesFromMap(map[string]uint64{"_not_exists": 1})
		aggs = append(aggs, hist)
	}

	hists := make(map[seq.MID]uint64)
	for i := 0; i < size; i++ {
		hists[seq.NewID(getTime(), uint64(i%10)).MID]++
	}

	var errs []seq.ErrorSource
	for i := 0; i < rand.N(100); i++ {
		src := rand.N[uint64](math.MaxUint64)
		errs = append(errs, seq.ErrorSource{ErrStr: "error", Source: src})
	}

	return seq.QPR{
		IDs:       ids,
		Histogram: hists,
		Aggs:      aggs,
		Total:     uint64(size),
		Errors:    errs,
	}
}

func stringColumn(values ...string) seq.StringColumn {
	column := seq.NewStringColumn(len(values))
	for _, value := range values {
		column.AppendString(value)
	}
	return column
}

func midsColumn(values ...seq.MID) seq.Int64Column {
	column := seq.NewInt64Column(len(values))
	for _, value := range values {
		column.Append(int64(value))
	}
	return column
}

func aggSamplesFromMap(other map[string]uint64) seq.AggregatableSamples {
	samplesByBin := make([]seq.AggBinSamples, 0, len(other))
	for k, cnt := range other {
		hist := seq.NewSamplesContainers()
		hist.Total = int64(cnt)
		samplesByBin = append(samplesByBin, seq.AggBinSamples{
			Bin:     seq.AggBin{Token: k},
			Samples: *hist,
		})
	}
	seq.SortAggBinSamples(samplesByBin)
	result := seq.AggregatableSamplesFromBins(samplesByBin)
	result.NotExists = int64(other["_not_exists"])
	return result
}
