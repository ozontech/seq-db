package seq

import (
	"fmt"
	"math"
	"slices"

	"github.com/valyala/fastrand"
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

func (v *AggValues) appendFrom(values *AggValues, row int, total, notExists int64, remap []uint32) {
	switch v.Func {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		v.Totals.Append(total)
	}
	v.appendFromRest(values, row, notExists, remap)
}

func (v *AggValues) appendFromRest(values *AggValues, row int, notExists int64, remap []uint32) {
	switch v.Func {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		v.NotExists.Append(notExists)
		v.Sums.Append(values.Sums.At(row))
		v.Mins.Append(values.Mins.At(row))
		v.Maxs.Append(values.Maxs.At(row))
		if v.Func == AggFuncQuantile {
			v.Quantiles.AppendFrom(&values.Quantiles, row, nil)
		}
	case AggFuncUniqueCount:
		v.NotExists.Append(notExists)
		v.UniqueIDs.AppendFrom(&values.UniqueIDs, row, func(ids []uint32) []uint32 {
			return remapUniqueIDs(ids, remap)
		})
	}
}

func (v *AggValues) mergeInto(dstRow int, cursor *mergeCursor, remap []uint32) {
	src := cursor.aggs
	srcRow := cursor.idx
	switch v.Func {
	case AggFuncCount:
		v.Totals.Add(dstRow, cursor.total())
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		v.Totals.Add(dstRow, cursor.total())
		v.NotExists.Add(dstRow, cursor.notExists())
		v.Sums.Add(dstRow, src.Values.Sums.At(srcRow))
		v.Mins.Add(dstRow, min(v.Mins.At(dstRow), src.Values.Mins.At(srcRow))-v.Mins.At(dstRow))
		v.Maxs.Add(dstRow, max(v.Maxs.At(dstRow), src.Values.Maxs.At(srcRow))-v.Maxs.At(dstRow))
		if v.Func == AggFuncQuantile {
			v.mergeQuantileInto(dstRow, src.Values.Quantiles, srcRow)
		}
	case AggFuncUniqueCount:
		v.Totals.Add(dstRow, cursor.total())
		v.NotExists.Add(dstRow, cursor.notExists())
		v.mergeUniqueIDsInto(dstRow, src.Values.UniqueIDs, srcRow, remap)
	case AggFuncUnique:
	default:
		panic(fmt.Errorf("unsupported aggregation func %s", v.Func))
	}
}

func (v *AggValues) mergeQuantileInto(dstRow int, right ArraysColumn[float64], srcRow int) {
	left := v.Quantiles.Row(dstRow)
	rng := fastrand.RNG{}
	rng.Seed(uint32(dstRow) + 73)
	rightValues := right.Row(srcRow)
	merged := make([]float64, 0, len(left)+len(rightValues))
	merged = append(merged, left...)
	for _, value := range rightValues {
		if len(merged) < maxHistogramSamples {
			merged = append(merged, value)
			continue
		}
		merged[rng.Uint32()%maxHistogramSamples] = value
	}

	v.Quantiles.Replace(dstRow, merged)
}

func (v *AggValues) quantile(row int, quantile float64) float64 {
	if quantile < 0 || quantile > 1 {
		panic("BUG: invalid quantile")
	}
	samples := v.Quantiles.Row(row)
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

func (v *AggValues) mergeUniqueIDsInto(
	dstRow int,
	right ArraysColumn[uint32],
	srcRow int,
	remap []uint32,
) {
	rightIDs := remapUniqueIDs(right.Row(srcRow), remap)
	leftIDs := v.UniqueIDs.Row(dstRow)
	merged := mergeUniqueIDs(leftIDs, rightIDs)
	v.UniqueIDs.Replace(dstRow, merged)
}

func (v *AggValues) uniqueCount(row int) int {
	return len(v.UniqueIDs.Row(row))
}

func remapUniqueIDs(ids, remap []uint32) []uint32 {
	if len(ids) == 0 || len(remap) == 0 {
		return ids
	}
	remapped := make([]uint32, len(ids))
	for i, id := range ids {
		remapped[i] = remap[id]
	}
	return remapped
}

func mergeUniqueIDs(left, right []uint32) []uint32 {
	merged := make([]uint32, 0, len(left)+len(right))
	leftIdx, rightIdx := 0, 0
	for leftIdx < len(left) && rightIdx < len(right) {
		switch {
		case left[leftIdx] < right[rightIdx]:
			merged = append(merged, left[leftIdx])
			leftIdx++
		case left[leftIdx] > right[rightIdx]:
			merged = append(merged, right[rightIdx])
			rightIdx++
		default:
			merged = append(merged, left[leftIdx])
			leftIdx++
			rightIdx++
		}
	}
	merged = append(merged, left[leftIdx:]...)
	merged = append(merged, right[rightIdx:]...)
	return merged
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

func (v *AggValues) Validate(rows int) error {
	if v.Func > AggFuncUniqueCount {
		return fmt.Errorf("unsupported aggregation func %d", v.Func)
	}
	checkRows := func(name string, length int, required bool) error {
		if required {
			if length != rows {
				return fmt.Errorf("invalid %s column length %d; want %d", name, length, rows)
			}
			return nil
		}
		if length != 0 {
			return fmt.Errorf("unexpected %s column length %d; want 0", name, length)
		}
		return nil
	}
	switch v.Func {
	case AggFuncCount:
		if err := checkRows("totals", v.Totals.Len(), true); err != nil {
			return err
		}
		if err := checkRows("not_exists", v.NotExists.Len(), false); err != nil {
			return err
		}
		if err := v.validateEmptyNumbers(); err != nil {
			return err
		}
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		if err := checkRows("totals", v.Totals.Len(), true); err != nil {
			return err
		}
		if err := checkRows("not_exists", v.NotExists.Len(), true); err != nil {
			return err
		}
		if err := checkRows("sums", v.Sums.Len(), true); err != nil {
			return err
		}
		if err := checkRows("mins", v.Mins.Len(), true); err != nil {
			return err
		}
		if err := checkRows("maxs", v.Maxs.Len(), true); err != nil {
			return err
		}
	case AggFuncUniqueCount:
		if err := checkRows("totals", v.Totals.Len(), true); err != nil {
			return err
		}
		if err := checkRows("not_exists", v.NotExists.Len(), true); err != nil {
			return err
		}
		if err := v.validateEmptyNumbers(); err != nil {
			return err
		}
	case AggFuncUnique:
		if err := checkRows("totals", v.Totals.Len(), false); err != nil {
			return err
		}
		if err := checkRows("not_exists", v.NotExists.Len(), false); err != nil {
			return err
		}
		if err := v.validateEmptyNumbers(); err != nil {
			return err
		}
	}
	if v.Func == AggFuncQuantile {
		if err := v.Quantiles.Validate(rows); err != nil {
			return fmt.Errorf("invalid quantiles column: %w", err)
		}
	} else if err := v.Quantiles.Validate(0); err != nil {
		return fmt.Errorf("invalid quantiles column: %w", err)
	}
	if v.Func == AggFuncUniqueCount {
		if err := v.UniqueIDs.Validate(rows); err != nil {
			return fmt.Errorf("invalid unique IDs column: %w", err)
		}
	} else if err := v.UniqueIDs.Validate(0); err != nil {
		return fmt.Errorf("invalid unique IDs column: %w", err)
	}
	return nil
}

func (v *AggValues) validateEmptyNumbers() error {
	if v.Sums.Len() != 0 {
		return fmt.Errorf("unexpected sums column length %d; want 0", v.Sums.Len())
	}
	if v.Mins.Len() != 0 {
		return fmt.Errorf("unexpected mins column length %d; want 0", v.Mins.Len())
	}
	if v.Maxs.Len() != 0 {
		return fmt.Errorf("unexpected maxs column length %d; want 0", v.Maxs.Len())
	}
	return nil
}

func validateOffsets(offsets []int, values, rows int) error {
	if len(offsets) != rows+1 {
		return fmt.Errorf("invalid offsets length %d; want %d", len(offsets), rows+1)
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] || offsets[len(offsets)-1] != values {
			return fmt.Errorf("invalid offsets")
		}
	}
	return nil
}
