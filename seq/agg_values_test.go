package seq

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fastrand"
)

func TestArraysColumnReplace(t *testing.T) {
	column := ArraysColumn[int]{
		Values:  []int{1, 2, 3},
		Offsets: []int{0, 2},
	}

	column.Replace(0, []int{1, 2, 3, 4})
	require.Equal(t, []int{1, 2, 3, 4}, column.Values)
	require.Equal(t, []int{0, 4}, column.Offsets)

	column.Replace(0, []int{5})
	require.Equal(t, []int{5}, column.Values)
	require.Equal(t, []int{0, 1}, column.Offsets)
}

func TestAggValuesColumnLayout(t *testing.T) {
	for _, fn := range []AggFunc{
		AggFuncCount,
		AggFuncSum,
		AggFuncMin,
		AggFuncMax,
		AggFuncAvg,
		AggFuncQuantile,
		AggFuncUnique,
		AggFuncUniqueCount,
	} {
		t.Run(fn.String(), func(t *testing.T) {
			samples := []SamplesContainer{
				{Total: 2, NotExists: 1, Min: 1, Max: 2, Sum: 3, Samples: []float64{1}, Values: map[uint32]struct{}{1: {}}},
				{Total: 3, NotExists: 2, Min: 2, Max: 3, Sum: 5, Samples: []float64{2}, Values: map[uint32]struct{}{2: {}}},
			}
			values := NewAggValuesFromSamples(fn, samples)
			require.NotNil(t, values.Totals)
			require.NotNil(t, values.NotExists)
			require.NoError(t, values.Validate(len(samples)))

			hasTotals := false
			hasNotExists := false
			hasNumbers := false
			hasQuantiles := false
			hasUniqueIDs := false
			switch fn {
			case AggFuncCount:
				hasTotals = true
			case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg:
				hasTotals, hasNotExists, hasNumbers = true, true, true
			case AggFuncQuantile:
				hasTotals, hasNotExists, hasNumbers, hasQuantiles = true, true, true, true
			case AggFuncUniqueCount:
				hasTotals, hasNotExists, hasUniqueIDs = true, true, true
			case AggFuncUnique:
			}
			checkLen := func(actual int, active bool) {
				if active {
					require.Equal(t, len(samples), actual)
				} else {
					require.Zero(t, actual)
				}
			}
			checkLen(values.Totals.Len(), hasTotals)
			checkLen(values.NotExists.Len(), hasNotExists)
			checkLen(values.Sums.Len(), hasNumbers)
			checkLen(values.Mins.Len(), hasNumbers)
			checkLen(values.Maxs.Len(), hasNumbers)
			checkLen(values.Quantiles.Rows(), hasQuantiles)
			checkLen(values.UniqueIDs.Rows(), hasUniqueIDs)
		})
	}
}

func TestAggValuesMergeQuantileRows(t *testing.T) {
	left := NewAggValuesFromSamples(AggFuncQuantile, []SamplesContainer{
		{Samples: []float64{10, 20}},
		{Samples: []float64{30, 40}},
	})
	right := NewAggValuesFromSamples(AggFuncQuantile, []SamplesContainer{
		{Samples: []float64{15, 25}},
		{Samples: []float64{50, 60}},
	})

	for row := range left.Quantiles.Rows() {
		left.mergeQuantileInto(row, right.Quantiles, row)
	}

	require.Equal(t, []float64{10, 20, 15, 25, 30, 40, 50, 60}, left.Quantiles.Values)
	require.Equal(t, []int{0, 4, 8}, left.Quantiles.Offsets)
	require.NoError(t, left.Quantiles.Validate(left.Quantiles.Rows()))
}

func TestAggValuesMergeQuantileEmptyRows(t *testing.T) {
	left := NewAggValuesFromSamples(AggFuncQuantile, []SamplesContainer{
		{Samples: []float64{10}},
		{},
		{Samples: []float64{30}},
	})
	right := NewAggValuesFromSamples(AggFuncQuantile, []SamplesContainer{
		{},
		{Samples: []float64{20}},
		{},
	})

	for row := range left.Quantiles.Rows() {
		left.mergeQuantileInto(row, right.Quantiles, row)
	}

	require.Equal(t, []float64{10, 20, 30}, left.Quantiles.Values)
	require.Equal(t, []int{0, 1, 2, 3}, left.Quantiles.Offsets)
	require.NoError(t, left.Quantiles.Validate(left.Quantiles.Rows()))
}

func TestAggValuesMergeQuantileReservoirLimit(t *testing.T) {
	leftValues := make([]float64, maxHistogramSamples)
	for i := range leftValues {
		leftValues[i] = float64(i)
	}
	rightValues := []float64{
		float64(maxHistogramSamples),
		float64(maxHistogramSamples + 1),
		float64(maxHistogramSamples + 2),
	}

	left := NewAggValuesFromSamples(AggFuncQuantile, []SamplesContainer{
		{Samples: leftValues},
	})
	right := NewAggValuesFromSamples(AggFuncQuantile, []SamplesContainer{
		{Samples: rightValues},
	})
	left.mergeQuantileInto(0, right.Quantiles, 0)

	expected := slices.Clone(leftValues)
	rng := fastrand.RNG{}
	rng.Seed(uint32(73))
	for _, value := range rightValues {
		expected[rng.Uint32()%maxHistogramSamples] = value
	}

	require.Len(t, left.Quantiles.Row(0), maxHistogramSamples)
	require.Equal(t, []int{0, maxHistogramSamples}, left.Quantiles.Offsets)
	require.Equal(t, expected, left.Quantiles.Row(0))
	require.NoError(t, left.Quantiles.Validate(left.Quantiles.Rows()))
}

func TestAggValuesMergeUniqueIDRows(t *testing.T) {
	left := NewAggValuesFromSamples(AggFuncUniqueCount, []SamplesContainer{
		{Values: map[uint32]struct{}{1: {}, 3: {}, 5: {}}},
		{},
		{Values: map[uint32]struct{}{7: {}}},
	})
	right := NewAggValuesFromSamples(AggFuncUniqueCount, []SamplesContainer{
		{Values: map[uint32]struct{}{2: {}, 3: {}, 9: {}}},
		{Values: map[uint32]struct{}{4: {}}},
		{},
	})

	for row := range left.UniqueIDs.Rows() {
		left.mergeUniqueIDsInto(row, right.UniqueIDs, row, nil)
	}

	require.Equal(t, []uint32{1, 2, 3, 5, 9, 4, 7}, left.UniqueIDs.Values)
	require.Equal(t, []int{0, 5, 6, 7}, left.UniqueIDs.Offsets)
	require.NoError(t, left.UniqueIDs.Validate(left.UniqueIDs.Rows()))
	require.Len(t, left.UniqueIDs.Row(0), 5)
	require.Equal(t, []uint32{4}, left.UniqueIDs.Row(1))
	require.Equal(t, []uint32{7}, left.UniqueIDs.Row(2))
}

func TestAggValuesMergeUniqueIDsWithRemap(t *testing.T) {
	left := NewAggValuesFromSamples(AggFuncUniqueCount, []SamplesContainer{
		{Values: map[uint32]struct{}{1: {}, 3: {}}},
	})
	right := NewAggValuesFromSamples(AggFuncUniqueCount, []SamplesContainer{
		{Values: map[uint32]struct{}{2: {}, 4: {}}},
	})
	remap := []uint32{10, 11, 12, 13, 20}

	left.mergeUniqueIDsInto(0, right.UniqueIDs, 0, remap)

	require.Equal(t, []uint32{1, 3, 12, 20}, left.UniqueIDs.Row(0))
	require.NoError(t, left.UniqueIDs.Validate(left.UniqueIDs.Rows()))
}

func TestAggValuesMergeIntoUniqueSamples(t *testing.T) {
	leftAgg := AggregatableSamplesFromBins([]AggBinSamples{
		{
			Bin: AggBin{Token: "row-0"},
			Samples: SamplesContainer{
				Samples: []float64{1, 3},
				Values:  map[uint32]struct{}{1: {}, 3: {}},
			},
		},
		{
			Bin: AggBin{Token: "row-1"},
			Samples: SamplesContainer{
				Samples: []float64{5},
				Values:  map[uint32]struct{}{7: {}},
			},
		},
	})
	rightAgg := AggregatableSamplesFromBins([]AggBinSamples{
		{
			Bin: AggBin{Token: "row-0"},
			Samples: SamplesContainer{
				Samples: []float64{2, 4},
				Values:  map[uint32]struct{}{2: {}, 4: {}},
			},
		},
		{
			Bin: AggBin{Token: "row-1"},
			Samples: SamplesContainer{
				Samples: []float64{6},
				Values:  map[uint32]struct{}{8: {}},
			},
		},
	})
	cursor := mergeCursor{aggs: &rightAgg, idx: 1, remap: []uint32{
		0, 0, 2, 0, 4, 0, 0, 6, 8,
	}}

	leftAgg.Values.mergeInto(1, &cursor, cursor.remap)

	require.Equal(t, []uint32{7, 8}, leftAgg.Values.UniqueIDs.Row(1))
	require.NoError(t, leftAgg.Values.UniqueIDs.Validate(leftAgg.RowsCount()))
}
