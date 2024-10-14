package frac

import (
	"slices"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

// maxReusableInverserSliceSize has a certain magical value, chosen empirically and relevant at the moment.
// In the future this should be replaced by some dynamically calculated value based on statistics.
// Or perhaps we will get rid of the reuse of the slice and maybe the inverser altogether.
const maxReusableInverserSliceSize = 2_000_000

var inverserSlicePool = sync.Pool{}

type inverser struct {
	values    []uint32
	inversion []int
}

func newInverser(values []uint32) *inverser {
	is := inverser{
		values:    values,
		inversion: getSlice(int(slices.Max(values)) + 1),
	}
	for i, v := range values {
		is.inversion[v] = i + 1
	}
	return &is
}

func (is *inverser) Len() int {
	// Explanation of "plus one" in len:
	//
	// 1. Inverser responsible for a mapping, for example:
	// 1 2 3 4 5 - external lids
	// 6 5 3 2 1 - internal lids
	//
	// 2. But we consider that there is unused virtual zero item:
	//   0     1 2 3 4 5 - external lids
	// unused  6 5 3 2 1 - internal lids
	//
	// 3. Actual representation in values field is:
	// 0 1 2 3 4 - keys
	// 6 5 3 2 1 - values
	//
	// Thats why we need plus one to len(values),
	// accounting virtual zero lid
	return len(is.values) + 1
}

func (is *inverser) Inverse(k uint32) (int, bool) {
	if int(k) >= len(is.inversion) {
		return 0, false
	}
	v := is.inversion[k]
	return v, v > 0
}

func (is *inverser) Revert(i uint32) uint32 {
	return is.values[i-1]
}

func (is *inverser) Release() {
	if cap(is.inversion) > maxReusableInverserSliceSize {
		return
	}
	inverserSlicePool.Put(&is.inversion)
}

func getSlice(size int) []int {
	if result := reuseSlice(size); result != nil {
		return result
	}
	logger.Info("recreate Inverser slice", zap.Int("size", size))
	if size > maxReusableInverserSliceSize {
		return make([]int, size)
	}
	return make([]int, size, maxReusableInverserSliceSize)
}

func reuseSlice(size int) []int {
	if size > maxReusableInverserSliceSize {
		return nil
	}

	item := inverserSlicePool.Get()

	if item == nil {
		return nil
	}

	slice, ok := item.(*[]int)

	if !ok {
		return nil
	}

	if cap(*slice) < size {
		return nil
	}

	return cleanSlice((*slice)[:size])
}

func cleanSlice(slice []int) []int {
	for i := range slice {
		slice[i] = 0
	}
	return slice
}
