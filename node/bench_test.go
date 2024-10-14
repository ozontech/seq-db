package node

import (
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

// bench for base point
// you can't go faster
func BenchmarkCopy(b *testing.B) {
	res := make([]uint32, b.N)
	n := newNodeStaticSize(b.N)
	b.ResetTimer()
	copy(res, n.data)
}

// base point
func BenchmarkIterate(b *testing.B) {
	res := make([]uint32, b.N)
	n := newNodeStaticSize(b.N)
	b.ResetTimer()
	for i, v := range n.data {
		res[i] = v
	}
}

func BenchmarkStatic(b *testing.B) {
	res := make([]uint32, 0, b.N)
	n := newNodeStaticSize(b.N)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N)
}

func BenchmarkNot(b *testing.B) {
	v, last := Generate(b.N)
	res := make([]uint32, 0, last+1)
	n := NewNot(NewStatic(v, false), 1, last, false)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), int(last)+1)
}

func BenchmarkNotEmpty(b *testing.B) {
	res := make([]uint32, 0, b.N*2)
	n := NewNot(NewStatic(nil, false), 1, uint32(b.N), false)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N*2)
}

func BenchmarkOr(b *testing.B) {
	res := make([]uint32, 0, b.N*2)
	n := NewOr(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N*2)
}

func BenchmarkAnd(b *testing.B) {
	res := make([]uint32, 0, b.N)
	n := NewAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N)
}

func BenchmarkNAnd(b *testing.B) {
	res := make([]uint32, 0, b.N)
	n := NewNAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N)
}

func BenchmarkAndTree(b *testing.B) {
	n1 := NewAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n2 := NewAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n3 := NewAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n4 := NewAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n12 := NewAnd(n1, n2, false)
	n34 := NewAnd(n3, n4, false)
	n := NewAnd(n12, n34, false)
	res := make([]uint32, 0, b.N)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N)
}

func BenchmarkOrTree(b *testing.B) {
	n1 := NewOr(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n2 := NewOr(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n3 := NewOr(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n4 := NewOr(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n12 := NewOr(n1, n2, false)
	n34 := NewOr(n3, n4, false)
	n := NewOr(n12, n34, false)
	res := make([]uint32, 0, b.N*8)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N*8)
}

func BenchmarkComplex(b *testing.B) {
	res := make([]uint32, 0, b.N*2)
	n1 := NewAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n2 := NewOr(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n3 := NewNAnd(newNodeStaticSize(b.N), newNodeStaticSize(b.N), false)
	n12 := NewOr(n1, n2, false)
	n := NewAnd(n12, n3, false)
	b.ResetTimer()
	res = readAllInto(n, res)
	assert.Equal(b, cap(res), b.N*2)
}
