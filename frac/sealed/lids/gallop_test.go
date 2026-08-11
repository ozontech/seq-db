package lids

import (
	"math/rand/v2"
	"sort"
	"testing"
)

func TestGallopAgainstSortSearch(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 200000; i++ {
		n := 1 + r.IntN(200)
		a := make([]uint32, n)
		v := uint32(r.IntN(3))
		for j := range a {
			v += uint32(r.IntN(4)) // duplicates allowed
			a[j] = v
		}
		q := uint32(r.IntN(int(v) + 3))

		wantGeq := sort.Search(len(a), func(k int) bool { return a[k] >= q })
		if got := searchGeqGallop(a, q); got != wantGeq {
			t.Fatalf("searchGeqGallop(%v, %d) = %d, want %d", a, q, got, wantGeq)
		}

		wantGt := sort.Search(len(a), func(k int) bool { return a[k] > q })
		if got := searchGtGallopTail(a, q); got != wantGt {
			t.Fatalf("searchGtGallopTail(%v, %d) = %d, want %d", a, q, got, wantGt)
		}
	}
}
