package node

import (
	"fmt"
	"testing"
	"time"
)

var (
	// Setup dataset size and data itself.
	lidsCount uint32 = 100_000
	// lids is a sorted slice [1; 1 + lidsCount);
	lids, last = generate(lidsCount)
)

func generate(n uint32) ([]uint32, uint32) {
	v := make([]uint32, n)
	last := uint32(1)
	for i := 0; i < len(v); i++ {
		v[i] = last
		last += 1
	}
	return v, last
}

// bench for base point
// you can't go faster
func BenchmarkCopy(b *testing.B) {
	b.SkipNow()

	res := make([]uint32, b.N)
	var n *staticAsc = nil
	b.ResetTimer()
	copy(res, n.data)
}

// base point
func BenchmarkIterate(b *testing.B) {
	b.SkipNow()

	res := make([]uint32, b.N)
	var n *staticAsc = nil
	b.ResetTimer()
	for i, v := range n.data {
		res[i] = v
	}
}

func BenchmarkStatic(b *testing.B) {
	b.Run("asc", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			n := NewStatic(lids, false)
			b.StartTimer()

			all(n)
		}
	})

	b.Run("desc", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			n := NewStatic(lids, true)
			b.StartTimer()

			all(n)
		}
	})
}

func BenchmarkNot(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		n := NewNot(
			NewStatic(lids, false),
			1, last, false,
		)
		b.StartTimer()

		all(n)
	}
}

func BenchmarkNotEmpty(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		n := NewNot(
			NewStatic(nil, false),
			1, uint32(lidsCount), false,
		)
		b.StartTimer()

		all(n)
	}
}

func BenchmarkOr(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		n := NewOr(
			NewStatic(lids, false),
			NewStatic(lids, false),
			false,
		)
		b.StartTimer()

		all(n)
	}
}

func BenchmarkAnd(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		n := NewAnd(
			NewStatic(lids, false),
			NewStatic(lids, false),
			false,
		)
		b.StartTimer()

		all(n)
	}
}

func BenchmarkNAnd(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		n := NewNAnd(
			NewStatic(lids, false),
			NewStatic(lids, false),
			false,
		)
		b.StartTimer()

		all(n)
	}
}

func BenchmarkAndTree(b *testing.B) {
	for b.Loop() {
		b.StopTimer()

		n1 := NewAnd(NewStatic(lids, false), NewStatic(lids, false), false)
		n2 := NewAnd(NewStatic(lids, false), NewStatic(lids, false), false)
		n3 := NewAnd(NewStatic(lids, false), NewStatic(lids, false), false)
		n4 := NewAnd(NewStatic(lids, false), NewStatic(lids, false), false)

		n12 := NewAnd(n1, n2, false)
		n34 := NewAnd(n3, n4, false)

		n := NewAnd(n12, n34, false)
		b.StartTimer()

		all(n)
	}
}

func BenchmarkOrTree(b *testing.B) {
	for b.Loop() {
		b.StopTimer()

		n1 := NewOr(NewStatic(lids, false), NewStatic(lids, false), false)
		n2 := NewOr(NewStatic(lids, false), NewStatic(lids, false), false)
		n3 := NewOr(NewStatic(lids, false), NewStatic(lids, false), false)
		n4 := NewOr(NewStatic(lids, false), NewStatic(lids, false), false)

		n12 := NewOr(n1, n2, false)
		n34 := NewOr(n3, n4, false)

		n := NewOr(n12, n34, false)
		b.StartTimer()

		all(n)
	}
}

// FIXME(dkharms): What is wrong here?
// One iteration finishes in 20ns - that's impossible.
func BenchmarkComplex(b *testing.B) {
	b.SkipNow()

	for b.Loop() {
		b.StopTimer()

		n1 := NewAnd(NewStatic(lids, false), NewStatic(lids, false), false)
		n2 := NewOr(NewStatic(lids, false), NewStatic(lids, false), false)
		n3 := NewNAnd(NewStatic(lids, false), NewStatic(lids, false), false)

		n12 := NewOr(n1, n2, false)

		n := NewAnd(n12, n3, false)
		b.StartTimer()

		start := time.Now()
		all(n)
		fmt.Printf("time.Since(start): %v\n", time.Since(start))
	}
}
