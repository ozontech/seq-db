//go:build !race

package frac

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInverserReuseSlice(t *testing.T) {
	// Disable GC to avoid sync.Pool cleanup
	debug.SetGCPercent(-1)
	defer debug.SetGCPercent(100)

	// create, fill and release
	size := 10
	inv := &inverser{inversion: getSlice(size)}
	assert.Equal(t, size, len(inv.inversion))
	assert.Equal(t, maxReusableInverserSliceSize, cap(inv.inversion))
	assert.Equal(t, make([]int, size), inv.inversion)
	for i := range inv.inversion {
		inv.inversion[i] = i
	}
	inv.Release()

	// success reuse 1
	size = 100
	inv.inversion = reuseSlice(size)
	assert.Equal(t, size, len(inv.inversion))
	assert.Equal(t, maxReusableInverserSliceSize, cap(inv.inversion))
	assert.Equal(t, make([]int, size), inv.inversion)
	inv.Release()

	// success reuse 2
	size = maxReusableInverserSliceSize
	inv.inversion = reuseSlice(size)
	assert.Equal(t, size, len(inv.inversion))
	assert.Equal(t, maxReusableInverserSliceSize, cap(inv.inversion))
	assert.Equal(t, make([]int, size), inv.inversion)
	inv.Release()

	// unsuccess reuse 3 - too big size
	size = maxReusableInverserSliceSize + 1
	s := reuseSlice(size)
	assert.Equal(t, 0, len(s))
	assert.Equal(t, 0, cap(s))

	// success reuse 4
	size = maxReusableInverserSliceSize
	inv.inversion = reuseSlice(size)
	assert.Equal(t, size, len(inv.inversion))
	assert.Equal(t, maxReusableInverserSliceSize, cap(inv.inversion))
	assert.Equal(t, make([]int, size), inv.inversion)

	// unsuccess reuse 5 - empty pool (not released earlier)
	size = 100
	s = reuseSlice(size)
	assert.Equal(t, 0, len(s))
	assert.Equal(t, 0, cap(s))
}
