package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

const benchRandSeed int64 = 1

func newNodeStaticSize(r *rand.Rand, size int) *staticAsc {
	data, _ := Generate(r, size)
	return &staticAsc{staticCursor: staticCursor{data: data}}
}

func Generate(r *rand.Rand, n int) ([]uint32, uint32) {
	v := make([]uint32, n)
	last := uint32(1)
	for i := 0; i < len(v); i++ {
		v[i] = last
		last += uint32(1 + r.Intn(5))
	}
	return v, last
}

func BenchmarkNot(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			r := rand.New(rand.NewSource(benchRandSeed))
			v, last := Generate(r, s)
			res := make([]uint32, 0, last+1)
			n := NewNot(NewStatic(v, false), NewDescLID(1), NewDescLID(last))

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
			n := NewNot(NewStatic(nil, false), NewDescLID(1), NewDescLID(uint32(s)))

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
			r := rand.New(rand.NewSource(benchRandSeed))
			res := make([]uint32, 0, s*2)
			n := NewOr(newNodeStaticSize(r, s), newNodeStaticSize(r, s))

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
			r := rand.New(rand.NewSource(benchRandSeed))
			res := make([]uint32, 0, s)
			n := NewAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))

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
			r := rand.New(rand.NewSource(benchRandSeed))
			res := make([]uint32, 0, s)
			n := NewNAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))

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
			r := rand.New(rand.NewSource(benchRandSeed))
			n1 := NewAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n2 := NewAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n3 := NewAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n4 := NewAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
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
			r := rand.New(rand.NewSource(benchRandSeed))
			n1 := NewOr(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n2 := NewOr(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n3 := NewOr(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n4 := NewOr(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
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

func BenchmarkComplex(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			r := rand.New(rand.NewSource(benchRandSeed))
			res := make([]uint32, 0, s*2)
			n1 := NewAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n2 := NewOr(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n3 := NewNAnd(newNodeStaticSize(r, s), newNodeStaticSize(r, s))
			n12 := NewOr(n1, n2)
			n := NewAnd(n12, n3)

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s*2)
		})
	}
}
