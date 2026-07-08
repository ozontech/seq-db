package node

import (
	"math"

	"github.com/RoaringBitmap/roaring/v2"
)

// Invert returns all LIDs in [from, to] (inclusive) that are not present in b.
func Invert(b LIDBatch, from, to uint32) LIDBatch {
	if from > to {
		return EmptyBatch()
	}
	bm := roaring.NewBitmap()
	bm.AddRange(uint64(from), uint64(to)+1)
	if !b.IsEmpty() {
		bm.AndNot(toBitmapBatch(b).bm)
	}
	return NewBitmapBatch(bm)
}

// And intersects two batches in the given document order and returns result and unprocessed parts (either left or right
// will be empty). For AND operation left and right residuals are equal to provided left or right batch, it's safe.
func And(left, right LIDBatch, desc bool) (result, leftResidual, rightResidual LIDBatch) {
	empty := EmptyBatch()
	if left.IsEmpty() || right.IsEmpty() {
		return empty, empty, empty
	}

	leftBm := toBitmapBatch(left)
	rightBm := toBitmapBatch(right)

	resultBm := leftBm.bm.Clone()
	resultBm.And(rightBm.bm)
	result = NewBitmapBatch(resultBm)

	// If left or right are slice batches, we must return leftBm and rightBm (bitmap copies), since
	// left or right might be intersected with another batch again soon.
	if desc {
		if leftBm.max > rightBm.max {
			return result, leftBm, empty
		}
		if rightBm.max > leftBm.max {
			return result, empty, rightBm
		}
		return result, empty, empty
	}

	if leftBm.min < rightBm.min {
		return result, leftBm, empty
	}
	if rightBm.min < leftBm.min {
		return result, empty, rightBm
	}
	return result, empty, empty
}

// AndNot finds "AND NOT" result for two batches and returns result and unprocessed parts.
func AndNot(reg, neg LIDBatch, desc bool) (result, regResidual, negResidual LIDBatch) {
	empty := EmptyBatch()
	if reg.IsEmpty() {
		return empty, empty, neg
	}
	if neg.IsEmpty() {
		return reg, empty, empty
	}

	regBm := toBitmapBatch(reg)
	negBm := toBitmapBatch(neg)

	resultBm := regBm.bm.Clone()
	resultBm.AndNot(negBm.bm)

	return truncateBatches(resultBm, regBm, negBm, desc)
}

// Or unions two batches in the given document order and returns result and unprocessed parts (either left or right
// will be empty).
func Or(left, right LIDBatch, desc bool) (result, leftResidual, rightResidual LIDBatch) {
	empty := EmptyBatch()
	if left.IsEmpty() {
		return right, empty, empty
	}
	if right.IsEmpty() {
		return left, empty, empty
	}

	leftBm := toBitmapBatch(left)
	rightBm := toBitmapBatch(right)

	resultBm := leftBm.bm.Clone()
	resultBm.Or(rightBm.bm)

	return truncateBatches(resultBm, leftBm, rightBm, desc)
}

// OrMulti unions multiple batches in the given document order and returns
// result and unprocessed parts for each input batch.
func OrMulti(batches []LIDBatch, desc bool) (result LIDBatch, residuals []LIDBatch) {
	residuals = make([]LIDBatch, len(batches))
	bmBatches := make([]*bitmapBatch, len(batches))
	nonEmptyBmBatches := make([]*bitmapBatch, 0, len(batches))
	for i, b := range batches {
		residuals[i] = EmptyBatch()
		if b.IsEmpty() {
			continue
		}
		bm := toBitmapBatch(b)
		bmBatches[i] = bm
		nonEmptyBmBatches = append(nonEmptyBmBatches, bm)
	}

	if len(nonEmptyBmBatches) == 0 {
		return EmptyBatch(), residuals
	}
	if len(nonEmptyBmBatches) == 1 {
		return nonEmptyBmBatches[0], residuals
	}

	bitmaps := make([]*roaring.Bitmap, len(nonEmptyBmBatches))
	for i, b := range nonEmptyBmBatches {
		bitmaps[i] = b.bm
	}

	resultBm := roaring.FastOr(bitmaps...)

	if desc {
		minMax := nonEmptyBmBatches[0].max
		for i := 1; i < len(nonEmptyBmBatches); i++ {
			if nonEmptyBmBatches[i].max < minMax {
				minMax = nonEmptyBmBatches[i].max
			}
		}
		resultBm.RemoveRange(uint64(minMax)+1, math.MaxUint64)
		for i, bm := range bmBatches {
			if bm == nil {
				continue
			}
			if bm.max > minMax {
				residuals[i] = bm.Narrow(minMax+1, math.MaxUint32)
			}
		}
		return NewBitmapBatch(resultBm), residuals
	}

	maxMin := nonEmptyBmBatches[0].min
	for i := 1; i < len(nonEmptyBmBatches); i++ {
		if nonEmptyBmBatches[i].min > maxMin {
			maxMin = nonEmptyBmBatches[i].min
		}
	}
	resultBm.RemoveRange(0, uint64(maxMin))
	for i, bm := range bmBatches {
		if bm == nil {
			continue
		}
		if bm.min < maxMin {
			residuals[i] = bm.Narrow(0, maxMin-1)
		}
	}
	return NewBitmapBatch(resultBm), residuals
}

func truncateBatches(result *roaring.Bitmap, left *bitmapBatch, right *bitmapBatch, desc bool) (LIDBatch, LIDBatch, LIDBatch) {
	if desc {
		if left.max > right.max {
			leftRes := left.Narrow(right.max+1, math.MaxUint32)
			result.RemoveRange(uint64(right.max)+1, math.MaxUint64)
			return NewBitmapBatch(result), leftRes, EmptyBatch()
		}
		if right.max > left.max {
			rightRes := right.Narrow(left.max+1, math.MaxUint32)
			result.RemoveRange(uint64(left.max)+1, math.MaxUint64)
			return NewBitmapBatch(result), EmptyBatch(), rightRes
		}
		return NewBitmapBatch(result), EmptyBatch(), EmptyBatch()
	}

	if left.min < right.min {
		leftRes := left.Narrow(0, right.min-1)
		result.RemoveRange(0, uint64(right.min))
		return NewBitmapBatch(result), leftRes, EmptyBatch()
	}
	if right.min < left.min {
		rightRes := right.Narrow(0, left.min-1)
		result.RemoveRange(0, uint64(left.min))
		return NewBitmapBatch(result), EmptyBatch(), rightRes
	}
	return NewBitmapBatch(result), EmptyBatch(), EmptyBatch()
}

func toBitmapBatch(b LIDBatch) *bitmapBatch {
	if b.IsEmpty() {
		panic("empty batch is not allowed to be cast to bitmap batch")
	}
	if bb, ok := b.(*bitmapBatch); ok {
		return bb
	}
	slice, ok := b.(*sliceBatch)
	if !ok {
		panic("unsupported batch type")
	}
	bm := roaring.NewBitmap()
	bm.AddMany(slice.lids)
	return &bitmapBatch{
		bm:  bm,
		min: slice.Min(),
		max: slice.Max(),
	}
}
