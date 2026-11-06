package seq

import (
	"fmt"
	"math"
	"slices"
)

// AggValues agg columns. All columns are non-nil (at least empty) for simplicity.
type AggValues struct {
	Func      AggFunc
	Totals    Int64Column
	NotExists Int64Column
	Sums      Float64Column
	Mins      Float64Column
	Maxs      Float64Column
	Quantiles ArraysColumn[float64]
	UniqueIDs ArraysColumn[uint32]
}

func newAggValues(fn AggFunc, capacity int) AggValues {
	values := AggValues{
		Func:      fn,
		Totals:    NewEmptyInt64Column(),
		NotExists: NewEmptyInt64Column(),
	}
	switch fn {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		values.Totals = NewInt64Column(capacity)
	}
	switch fn {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		values.NotExists = NewInt64Column(capacity)
	}
	switch fn {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		values.Sums = NewFloat64Column(capacity)
		values.Mins = NewFloat64Column(capacity)
		values.Maxs = NewFloat64Column(capacity)
	}
	if fn == AggFuncQuantile {
		values.Quantiles = newArraysColumn[float64](capacity)
	}
	if fn == AggFuncUniqueCount {
		values.UniqueIDs = newArraysColumn[uint32](capacity)
	}
	return values
}

func NewAggValuesFromSamples(fn AggFunc, samples []SamplesContainer) AggValues {
	values := newAggValues(fn, len(samples))
	for i := range samples {
		values.appendRow(&samples[i])
	}
	return values
}

func (v *AggValues) appendRow(samples *SamplesContainer) {
	switch v.Func {
	case AggFuncCount:
		v.Totals.Append(samples.Total)
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		v.Totals.Append(samples.Total)
		v.NotExists.Append(samples.NotExists)
		v.Sums.Append(samples.Sum)
		v.Mins.Append(samples.Min)
		v.Maxs.Append(samples.Max)
		if v.Func == AggFuncQuantile {
			v.Quantiles.Append(samples.Samples)
		}
	case AggFuncUniqueCount:
		v.Totals.Append(samples.Total)
		v.NotExists.Append(samples.NotExists)
		ids := make([]uint32, 0, len(samples.Values))
		for id := range samples.Values {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		v.UniqueIDs.Append(ids)
	case AggFuncUnique:
	default:
		panic(fmt.Errorf("unsupported aggregation func %s", v.Func))
	}
}

func (v *AggValues) quantile(row int, quantile float64) float64 {
	if quantile < 0 || quantile > 1 {
		panic("BUG: invalid quantile")
	}
	samples := v.Quantiles.At(row)
	if len(samples) == 0 {
		return math.NaN()
	}
	if quantile == 0 {
		return v.Mins.At(row)
	}
	if quantile == 1 {
		return v.Maxs.At(row)
	}
	samples = slices.Clone(samples)
	slices.Sort(samples)
	index := int(float64(len(samples)-1)*quantile + 0.5)
	return samples[index]
}

func (v *AggValues) uniqueCount(row int) int {
	return len(v.UniqueIDs.At(row))
}

func (v *AggValues) memUsage() int {
	return v.Totals.MemUsage() +
		v.NotExists.MemUsage() +
		v.Sums.MemUsage() +
		v.Mins.MemUsage() +
		v.Maxs.MemUsage() +
		v.Quantiles.MemUsage() +
		v.UniqueIDs.MemUsage()
}
