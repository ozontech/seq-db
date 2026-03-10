package node

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newNodeStaticSize(size int) *staticAsc {
	data, _ := Generate(size)
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

func BenchmarkNot(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			v, last := Generate(s)
			res := make([]uint32, 0, last+1)
			n := NewNot(NewStatic(v, false), 1, last, false)

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
			n := NewNot(NewStatic(nil, false), 1, uint32(s), false)

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
			n := NewOr(newNodeStaticSize(s), newNodeStaticSize(s), false)

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
			n := NewAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)

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
			n := NewNAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)

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
			n1 := NewAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n2 := NewAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n3 := NewAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n4 := NewAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n12 := NewAnd(n1, n2, false)
			n34 := NewAnd(n3, n4, false)
			n := NewAnd(n12, n34, false)
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
			n1 := NewOr(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n2 := NewOr(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n3 := NewOr(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n4 := NewOr(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n12 := NewOr(n1, n2, false)
			n34 := NewOr(n3, n4, false)
			n := NewOr(n12, n34, false)
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
			res := make([]uint32, 0, s*2)
			n1 := NewAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n2 := NewOr(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n3 := NewNAnd(newNodeStaticSize(s), newNodeStaticSize(s), false)
			n12 := NewOr(n1, n2, false)
			n := NewAnd(n12, n3, false)

			for b.Loop() {
				res = readAllInto(n, res)
			}

			assert.Equal(b, cap(res), s*2)
		})
	}
}
