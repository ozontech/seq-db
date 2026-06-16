package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type batchFactory func([]uint32) LIDBatch

var batchFactories = []struct {
	name  string
	build batchFactory
}{
	{name: "bitmap", build: NewBitmapBatchFromLids},
	{name: "slice", build: NewSliceBatch},
}

func TestLIDBatchNarrow(t *testing.T) {
	testCases := []struct {
		name     string
		input    []uint32
		minLID   uint32
		maxLID   uint32
		expected []uint32
	}{
		{
			name:     "empty batch",
			input:    nil,
			minLID:   0,
			maxLID:   10,
			expected: nil},
		{
			name:     "full range no-op",
			input:    []uint32{1, 5, 10},
			minLID:   0,
			maxLID:   math.MaxUint32,
			expected: []uint32{1, 5, 10}},
		{
			name:     "trim below",
			input:    []uint32{1, 5, 10, 15},
			minLID:   6,
			maxLID:   20,
			expected: []uint32{10, 15}},
		{
			name:     "exact min boundary",
			input:    []uint32{1, 5, 10, 15},
			minLID:   10,
			maxLID:   20,
			expected: []uint32{10, 15}},
		{
			name:     "trim above",
			input:    []uint32{1, 5, 10, 15},
			minLID:   0,
			maxLID:   10,
			expected: []uint32{1, 5, 10}},
		{
			name:     "trim both sides",
			input:    []uint32{1, 5, 10, 15, 20},
			minLID:   5,
			maxLID:   15,
			expected: []uint32{5, 10, 15}},
		{
			name:     "no overlap below",
			input:    []uint32{1, 5, 10},
			minLID:   11,
			maxLID:   20,
			expected: nil},
		{
			name:     "no overlap above",
			input:    []uint32{5, 10, 20},
			minLID:   0,
			maxLID:   4,
			expected: nil},
		{
			name:     "overlap at min",
			input:    []uint32{7, 10, 20, 25},
			minLID:   0,
			maxLID:   7,
			expected: []uint32{7}},
		{
			name:     "overlap at max",
			input:    []uint32{7, 10, 20, 25},
			minLID:   25,
			maxLID:   40,
			expected: []uint32{25}},
	}

	for _, impl := range batchFactories {
		t.Run(impl.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					b := impl.build(tc.input)
					got := b.Narrow(tc.minLID, tc.maxLID)
					assert.Equal(t, tc.expected, toSlice(got))
				})
			}
		})
	}
}

func TestBitmapBatchNarrow_NoCloneWhenUnchanged(t *testing.T) {
	src := NewBitmapBatchFromLids([]uint32{1, 5, 10, 15, 20})
	got := src.Narrow(1, 20)
	assert.Same(t, src, got)
}

func toSlice(b LIDBatch) []uint32 {
	if b == nil || b.IsEmpty() {
		return nil
	}
	it := b.Iter()
	out := make([]uint32, 0, b.Len())
	for {
		v, ok := it.Next()
		if !ok {
			break
		}
		out = append(out, v)
	}
	return out
}

func TestNextGeq(t *testing.T) {
	for _, impl := range batchFactories {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.build([]uint32{1, 5, 10, 15, 20, 21, 22, 26, 30})
			it := b.Iter()

			v, ok := it.NextGeq(1)
			require.True(t, ok)
			assert.Equal(t, uint32(1), v)

			// calling NextGeq with already seen value returns next value
			v, ok = it.NextGeq(1)
			require.True(t, ok)
			assert.Equal(t, uint32(5), v)

			v, ok = it.NextGeq(13)
			require.True(t, ok)
			assert.Equal(t, uint32(15), v)

			v, ok = it.NextGeq(22)
			require.True(t, ok)
			assert.Equal(t, uint32(22), v)

			_, ok = it.NextGeq(50)
			assert.False(t, ok)
		})
	}
}

func TestReverseNextGeq(t *testing.T) {
	for _, impl := range batchFactories {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.build([]uint32{3, 5, 10, 15, 20, 21, 22, 26, 30})
			it := b.ReverseIter()

			v, ok := it.NextGeq(1000)
			require.True(t, ok)
			assert.Equal(t, uint32(30), v)

			// calling NextGeq with already seen value returns next value
			v, ok = it.NextGeq(30)
			require.True(t, ok)
			assert.Equal(t, uint32(26), v)

			v, ok = it.NextGeq(20)
			require.True(t, ok)
			assert.Equal(t, uint32(20), v)

			v, ok = it.NextGeq(9)
			require.True(t, ok)
			assert.Equal(t, uint32(5), v)

			_, ok = it.NextGeq(2)
			assert.False(t, ok)
		})
	}
}

func TestBatchIter(t *testing.T) {
	for _, impl := range batchFactories {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.build([]uint32{1, 5, 10, 15, 20})
			it := b.Iter()

			var got []uint32
			for {
				v, ok := it.Next()
				if !ok {
					break
				}
				got = append(got, v)
			}
			assert.Equal(t, []uint32{1, 5, 10, 15, 20}, got)

			b = impl.build([]uint32{1, 5, 10, 15, 20})
			it = b.Iter()
			v, ok := it.NextGeq(11)
			require.True(t, ok)
			assert.Equal(t, uint32(15), v)

			b = impl.build([]uint32{1, 5, 10})
			it = b.Iter()
			_, ok = it.NextGeq(100)
			assert.False(t, ok)
		})
	}
}

func TestBatchReverseIter(t *testing.T) {
	for _, impl := range batchFactories {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.build([]uint32{1, 5, 10, 15, 20})
			it := b.ReverseIter()

			var got []uint32
			for {
				v, ok := it.Next()
				if !ok {
					break
				}
				got = append(got, v)
			}
			assert.Equal(t, []uint32{20, 15, 10, 5, 1}, got)

			b = impl.build([]uint32{1, 5, 10, 15, 20})
			it = b.ReverseIter()
			v, ok := it.NextGeq(11)
			require.True(t, ok)
			assert.Equal(t, uint32(10), v)

			b = impl.build([]uint32{1, 5, 10})
			it = b.ReverseIter()
			_, ok = it.NextGeq(0)
			assert.False(t, ok)
		})
	}
}
