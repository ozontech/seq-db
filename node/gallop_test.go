package node

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestGallopBoundaries(t *testing.T) {
	values := []uint32{0, 1, math.MaxUint32 - 1, math.MaxUint32}
	queries := []uint32{0, 1, 2, math.MaxUint32 - 1, math.MaxUint32}

	for mask := 1; mask < 1<<len(values); mask++ {
		var a []uint32
		for i, v := range values {
			if mask&(1<<i) != 0 {
				a = append(a, v)
			}
		}
		for _, q := range queries {
			wantGeq := sort.Search(len(a), func(i int) bool { return a[i] >= q })
			assert.Equal(t, wantGeq, searchGeqGallop(a, q), "GEQ a=%v, q=%v", a, q)

			wantGt := sort.Search(len(a), func(i int) bool { return a[i] > q })
			assert.Equal(t, wantGt, searchGtGallopTail(a, q), "GT a=%v, q=%v", a, q)
		}
	}
}
