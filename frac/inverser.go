package frac

import (
	"unsafe"

	"github.com/ozontech/seq-db/bytespool"
)

type inverser struct {
	buf       *bytespool.Buffer
	values    []uint32
	inversion []int
}

func newInverser(values []uint32, size int) *inverser {
	buf, inversion := getSlice(size)
	for i, v := range values {
		inversion[v] = i + 1
	}

	return &inverser{
		buf:       buf,
		values:    values,
		inversion: inversion,
	}
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
	bytespool.Release(is.buf)
	is.buf = nil
	is.inversion = nil
}

func getSlice(size int) (*bytespool.Buffer, []int) {
	const sizeOfInt = unsafe.Sizeof(int(0))
	buf := bytespool.Acquire(size * int(sizeOfInt))
	s := unsafe.Slice((*int)(unsafe.Pointer(unsafe.SliceData(buf.B))), size)
	clear(s)
	return buf, s
}
