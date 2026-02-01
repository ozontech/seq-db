package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newNodeStaticSizeRand(size int) *staticAsc {
	data, _ := Generate(size)
	return &staticAsc{staticCursor: staticCursor{data: data}}
}

func newNodeStaticSizeFixedDelta(size int, start int, delta int) *staticAsc {
	data, _ := GenerateFixedDelta(size, start, delta)
	return &staticAsc{staticCursor: staticCursor{data: data}}
}

func Generate(n int) ([]uint32, uint32) {
	v := make([]uint32, n)
	last := uint32(1)
	for i := 0; i < len(v); i++ {
		v[i] = last
		last += uint32(1 + rand.Intn(5))
	}
	return v, last
}

func GenerateFixedDelta(n, start, step int) ([]uint32, uint32) {
	v := make([]uint32, n)
	last := uint32(start)
	for i := 0; i < len(v); i++ {
		v[i] = last
		last += uint32(step)
	}
	return v, last
}

func BenchmarkNot(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			v, last := Generate(s)
			res := make([]uint32, 0, last+1)
			n := NewNot(NewStatic(v, false), NewCmpLIDOrderDesc(1), NewCmpLIDOrderDesc(last))

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), int(last)+1)
		})
	}
}

func BenchmarkNotEmpty(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			res := make([]uint32, 0, s*2)
			n := NewNot(NewStatic(nil, false), NewCmpLIDOrderDesc(1), NewCmpLIDOrderDesc(uint32(s)))

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s*2)
		})
	}

}

func BenchmarkOr(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			res := make([]uint32, 0, s*2)
			n := NewOr(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s*2)
		})
	}
}

func BenchmarkAnd(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			res := make([]uint32, 0, s)
			n := NewAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s)
		})
	}
}

func BenchmarkNAnd(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			res := make([]uint32, 0, s)
			n := NewNAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s)
		})
	}
}

func BenchmarkAndTree(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			n1 := NewAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n2 := NewAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n3 := NewAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n4 := NewAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n12 := NewAnd(n1, n2)
			n34 := NewAnd(n3, n4)
			n := NewAnd(n12, n34)
			res := make([]uint32, 0, s)

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s)
		})
	}
}

func BenchmarkOrTree(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			n1 := NewOr(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n2 := NewOr(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n3 := NewOr(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n4 := NewOr(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n12 := NewOr(n1, n2)
			n34 := NewOr(n3, n4)
			n := NewOr(n12, n34)
			res := make([]uint32, 0, s*8)

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s*8)

		})
	}
}

// BenchmarkOrTreeNextGeq checks the performance of NextGeq vs Next when no skipping occur and all node
// yield distinct values (no intersection between nodes)
func BenchmarkOrTreeNextGeq(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}
	// step is equal to total number of nodes, so that every node produces distinct values
	step := 8

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			n1 := NewOr(
				newNodeStaticSizeFixedDelta(s, 1, step),
				newNodeStaticSizeFixedDelta(s, 5, step))
			n2 := NewOr(
				newNodeStaticSizeFixedDelta(s, 2, step),
				newNodeStaticSizeFixedDelta(s, 6, step))
			n3 := NewOr(
				newNodeStaticSizeFixedDelta(s, 3, step),
				newNodeStaticSizeFixedDelta(s, 8, step))
			n4 := NewOr(
				newNodeStaticSizeFixedDelta(s, 4, step),
				newNodeStaticSizeFixedDelta(s, 7, step))
			n12 := NewOr(n1, n2)
			n34 := NewOr(n3, n4)
			n := NewOr(n12, n34)
			res := make([]uint32, 0, s*8)

			for b.Loop() {
				res = readAllIntoGeq(n, res)
			}

			assert.Equal(b, cap(res), s*8)

		})
	}
}

func BenchmarkComplex(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			res := make([]uint32, 0, s*2)
			n1 := NewAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n2 := NewOr(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n3 := NewNAnd(newNodeStaticSizeRand(s), newNodeStaticSizeRand(s))
			n12 := NewOr(n1, n2)
			n := NewAnd(n12, n3)

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s*2)
		})
	}
}
