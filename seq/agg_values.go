package seq

import (
	"fmt"
	"math"
	"slices"
)

// AggValues agg columns. All columns are non-nil (at least empty) for simplicity.
type AggValues struct {
	Func      AggFunc
	Totals    Column[int64]
	NotExists Column[int64]
	Sums      Column[float64]
	Mins      Column[float64]
	Maxs      Column[float64]
	Quantiles Column[[]float64]
	UniqueIDs Column[[]uint32]
}

func newAggValues(fn AggFunc, capacity int) AggValues {
	return AggValues{
		Func:      fn,
		Totals:    NewInt64Column(capacity),
		NotExists: NewInt64Column(capacity),
		Sums:      NewFloat64Column(capacity),
		Mins:      NewFloat64Column(capacity),
		Maxs:      NewFloat64Column(capacity),
		Quantiles: NewArraysColumn[float64](capacity),
		UniqueIDs: NewArraysColumn[uint32](capacity),
	}
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

// NewSamplesIter walks the values row by row. The returned container is reused
// between Next calls and must be consumed before the next one.
func (v *AggValues) NewSamplesIter() *samplesIter {
	it := &samplesIter{values: v}
	switch v.Func {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		it.totals = v.Totals.NewIter()
	}
	switch v.Func {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		it.notExists = v.NotExists.NewIter()
	}
	switch v.Func {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		it.sums = v.Sums.NewIter()
		it.mins = v.Mins.NewIter()
		it.maxs = v.Maxs.NewIter()
	}
	if v.Func == AggFuncQuantile {
		it.quantiles = v.Quantiles.NewIter()
	}
	if v.Func == AggFuncUniqueCount {
		it.uniqueIDs = v.UniqueIDs.NewIter()
	}
	return it
}

type samplesIter struct {
	values    *AggValues
	totals    Iter[int64]
	notExists Iter[int64]
	sums      Iter[float64]
	mins      Iter[float64]
	maxs      Iter[float64]
	quantiles Iter[[]float64]
	uniqueIDs Iter[[]uint32]
	samples   SamplesContainer
}

func (it *samplesIter) Next() bool {
	s := &it.samples
	s.Total = 0
	s.NotExists = 0
	s.Sum = 0
	s.Min = 0
	s.Max = 0
	s.Samples = s.Samples[:0]
	if s.Values != nil {
		clear(s.Values)
	}
	if it.totals != nil && it.totals.Next() {
		s.Total = it.totals.Value()
	}
	if it.notExists != nil && it.notExists.Next() {
		s.NotExists = it.notExists.Value()
	}
	if it.sums != nil && it.sums.Next() {
		it.mins.Next()
		it.maxs.Next()
		s.Sum = it.sums.Value()
		s.Min = it.mins.Value()
		s.Max = it.maxs.Value()
	}
	if it.quantiles != nil && it.quantiles.Next() {
		s.Samples = append(s.Samples, it.quantiles.Value()...)
	}
	if it.uniqueIDs != nil && it.uniqueIDs.Next() {
		if s.Values == nil {
			s.Values = make(map[uint32]struct{})
		}
		for _, id := range it.uniqueIDs.Value() {
			s.Values[id] = struct{}{}
		}
	}
	return true
}

func (it *samplesIter) Samples() *SamplesContainer {
	return &it.samples
}

func quantile(samples *SamplesContainer, q float64) float64 {
	if q < 0 || q > 1 {
		panic("BUG: invalid quantile")
	}
	values := samples.Samples
	if len(values) == 0 {
		return math.NaN()
	}
	if q == 0 {
		return samples.Min
	}
	if q == 1 {
		return samples.Max
	}
	values = slices.Clone(values)
	slices.Sort(values)
	index := int(float64(len(values)-1)*q + 0.5)
	return values[index]
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
