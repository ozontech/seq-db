package util

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGallopSearchGeq(t *testing.T) {
	tests := []struct {
		name          string
		vals          []uint32
		geq           uint32
		expectedIdx   int
		expectedFound bool
	}{
		{
			name:          "empty",
			vals:          nil,
			geq:           1,
			expectedIdx:   0,
			expectedFound: false,
		},
		{
			name:          "single_found",
			vals:          []uint32{5},
			geq:           3,
			expectedIdx:   0,
			expectedFound: true,
		},
		{
			name:          "single_not_found",
			vals:          []uint32{2},
			geq:           5,
			expectedIdx:   0,
			expectedFound: false,
		},
		{
			name:          "first_element_greater",
			vals:          []uint32{1, 3, 5, 7, 9},
			geq:           0,
			expectedIdx:   0,
			expectedFound: true,
		},
		{
			name:          "first_element_equals",
			vals:          []uint32{1, 3, 5, 7, 9},
			geq:           1,
			expectedIdx:   0,
			expectedFound: true,
		},
		{
			name:          "middle_found_greater",
			vals:          []uint32{1, 3, 5, 7, 9},
			geq:           4,
			expectedIdx:   2,
			expectedFound: true,
		},
		{
			name:          "mid_found_exact",
			vals:          []uint32{1, 3, 5, 7, 9},
			geq:           5,
			expectedIdx:   2,
			expectedFound: true,
		},
		{
			name:          "last_found",
			vals:          []uint32{1, 3, 5, 7, 9},
			geq:           9,
			expectedIdx:   4,
			expectedFound: true,
		},
		{
			name:          "last_not_found",
			vals:          []uint32{1, 3, 5, 7, 9},
			geq:           10,
			expectedIdx:   0,
			expectedFound: false,
		},
		{
			name:          "gallop_then_binary_search",
			vals:          []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			geq:           17,
			expectedIdx:   16,
			expectedFound: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, ok := GallopSearchGeq(tt.vals, tt.geq)
			assert.Equal(t, tt.expectedFound, ok, "found")
			if tt.expectedFound {
				require.Less(t, idx, len(tt.vals), "index in range")
				assert.GreaterOrEqual(t, tt.vals[idx], tt.geq, "vals[idx] >= geq")
				if idx > 0 {
					assert.Less(t, tt.vals[idx-1], tt.geq, "element before is < geq")
				}
			}
			assert.Equal(t, tt.expectedIdx, idx)
		})
	}
}

func TestGallopSearchLeq(t *testing.T) {
	tests := []struct {
		name          string
		vals          []uint32
		leq           uint32
		expectedIdx   int
		expectedFound bool
	}{
		{
			name:          "empty",
			vals:          nil,
			leq:           5,
			expectedIdx:   0,
			expectedFound: false,
		},
		{
			name:          "single_found",
			vals:          []uint32{5},
			leq:           10,
			expectedIdx:   0,
			expectedFound: true,
		},
		{
			name:          "single_not_found",
			vals:          []uint32{5},
			leq:           3,
			expectedIdx:   0,
			expectedFound: false,
		},
		{
			name:          "last_element_less",
			vals:          []uint32{1, 3, 5, 7, 9},
			leq:           10,
			expectedIdx:   4,
			expectedFound: true,
		},
		{
			name:          "first_element_equal",
			vals:          []uint32{1, 3, 5, 7, 9},
			leq:           1,
			expectedIdx:   0,
			expectedFound: true,
		},
		{
			name:          "mid_less",
			vals:          []uint32{1, 3, 5, 7, 9},
			leq:           6,
			expectedIdx:   2,
			expectedFound: true,
		},
		{
			name:          "mid_equal",
			vals:          []uint32{1, 3, 5, 7, 9},
			leq:           5,
			expectedIdx:   2,
			expectedFound: true,
		},
		{
			name:          "below_first",
			vals:          []uint32{1, 3, 5, 7, 9},
			leq:           0,
			expectedIdx:   0,
			expectedFound: false,
		},
		{
			name:          "gallop_from_right_large",
			vals:          []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			leq:           17,
			expectedIdx:   16,
			expectedFound: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, ok := GallopSearchLeq(tt.vals, tt.leq)
			assert.Equal(t, tt.expectedFound, ok, "found")
			if tt.expectedFound {
				require.Less(t, idx, len(tt.vals), "index in range")
				assert.LessOrEqual(t, tt.vals[idx], tt.leq, "vals[idx] <= geq")
				if idx < len(tt.vals)-1 {
					assert.Greater(t, tt.vals[idx+1], tt.leq, "element after is > geq")
				}
			}
			assert.Equal(t, tt.expectedIdx, idx, "index")
		})
	}
}

func pickRandom(from, to uint32) uint32 {
	if from == to {
		return from
	}
	span := to - from + 1
	if span == 0 {
		return rand.Uint32()
	}
	return from + rand.Uint32()%span
}

// TestGallopSearchGeqVsSortSearch uses both gallop search and ordinary bin search to find a random number in a slice, then compares
func TestGallopSearchGeqVsSortSearch(t *testing.T) {
	const size = 100
	const numSearches = 50

	vals := make([]uint32, size)
	for i := range vals {
		vals[i] = rand.Uint32()
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })

	from, to := vals[0], vals[size-1]
	for i := 0; i < numSearches; i++ {
		x := pickRandom(from, to)
		expectedIdx := sort.Search(size, func(i int) bool { return vals[i] >= x })
		expectedFound := expectedIdx < size

		idx, found := GallopSearchGeq(vals, x)
		assert.Equal(t, expectedFound, found)
		if expectedFound {
			assert.Equal(t, expectedIdx, idx)
		}
	}
}

// TestGallopSearchLeqVsSortSearch uses both gallop search and ordinary bin search to find a random number in a slice, then compares
func TestGallopSearchLeqVsSortSearch(t *testing.T) {
	const size = 100
	const numSearches = 50

	vals := make([]uint32, size)
	for i := range vals {
		vals[i] = rand.Uint32()
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })

	from, to := vals[0], vals[size-1]
	for i := 0; i < numSearches; i++ {
		x := pickRandom(from, to)
		refIdx := sort.Search(size, func(i int) bool { return vals[i] > x }) - 1
		refFound := refIdx >= 0

		idx, found := GallopSearchLeq(vals, x)
		assert.Equal(t, refFound, found, "x=%d", x)
		if refFound {
			assert.Equal(t, refIdx, idx, "x=%d", x)
		}
	}
}
