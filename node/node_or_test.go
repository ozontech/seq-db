package node

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeOr_NextGeqAscending(t *testing.T) {
	left := NewStatic([]uint32{2, 7, 10, 20, 25, 26, 30, 50}, false)
	right := NewStatic([]uint32{1, 3, 4, 7, 9, 30, 40}, false)

	node := NewOr(left, right)

	id := node.NextGeq(NewCmpLIDOrderDesc(7))
	assert.Equal(t, uint32(7), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(7))
	assert.Equal(t, uint32(9), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(24))
	assert.Equal(t, uint32(25), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(30))
	assert.Equal(t, uint32(30), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(51))
	assert.True(t, id.IsNull())
}

// TestNodeOr_NextGeqCompatibility tests that just calling NextGeq with LID zero value passed as argument is equivalent to
// calling Next
func TestNodeOr_NextGeqCompatibility(t *testing.T) {
	for _, rev := range []bool{true, false} {
		left := []uint32{rand.Uint32N(10)}
		right := []uint32{rand.Uint32N(10)}

		for i := 1; i < 1000; i++ {
			left = append(left, left[i-1]+rand.Uint32N(10))
			right = append(right, right[i-1]+rand.Uint32N(10))
		}

		node := NewOr(NewStatic(left, rev), NewStatic(right, rev))
		nodeGeq := NewOr(NewStatic(left, rev), NewStatic(right, rev))

		var zero uint32
		if rev {
			zero = math.MaxUint32
		} else {
			zero = 0
		}

		for {
			lid := node.Next()
			lidGeq := nodeGeq.NextGeq(NewCmpLID(zero, rev))

			assert.Equal(t, lid, lidGeq)

			if lid.IsNull() {
				break
			}
		}
	}
}

// TestNodeOrAgg_NoDedup tests that nodeOrAgg yields values both from left and right for same lid.
func TestNodeOrAgg_NoDedup(t *testing.T) {
	left := NewSourcedNodeWrapper(NewStatic([]uint32{1, 5, 7}, false), 1)
	right := NewSourcedNodeWrapper(NewStatic([]uint32{5, 8}, false), 2)

	orAgg := NewNodeOrAgg(left, right, false)
	pairs := readAllSourced(orAgg)

	// expected sources for lid=5
	var sources []uint32

	for _, p := range pairs {
		id, src := p[0], p[1]
		if id == 5 {
			sources = append(sources, src)
		}
	}

	require.Len(t, sources, 2, "expected id 5 to be returned twice from both children")
	assert.ElementsMatch(t, []uint32{1, 2}, sources, "expected id 5 from both left and right sources")
}

func TestNodeOrAgg_MergeAscending(t *testing.T) {
	left := NewSourcedNodeWrapper(NewStatic([]uint32{1, 3, 5}, false), 0)
	right := NewSourcedNodeWrapper(NewStatic([]uint32{2, 4, 6}, false), 1)

	orAgg := NewNodeOrAgg(left, right, false)
	got := readAllSourced(orAgg)

	want := [][2]uint32{
		{1, 0},
		{2, 1},
		{3, 0},
		{4, 1},
		{5, 0},
		{6, 1},
	}

	assert.Equal(t, want, got)
}

func TestNodeOrAgg_MergeAscendingWithDups(t *testing.T) {
	left := NewSourcedNodeWrapper(NewStatic([]uint32{1, 2, 3, 5, 8}, false), 0)
	right := NewSourcedNodeWrapper(NewStatic([]uint32{2, 3, 4, 6, 8}, false), 1)

	orAgg := NewNodeOrAgg(left, right, false)
	got := readAllSourced(orAgg)

	want := [][2]uint32{
		{1, 0},
		{2, 1},
		{2, 0},
		{3, 1},
		{3, 0},
		{4, 1},
		{5, 0},
		{6, 1},
		{8, 1},
		{8, 0},
	}

	assert.Equal(t, want, got)
}

// TestNodeOrAgg_NextSourcedGeq tests we can navigate to a lid with NextGeq and do not skip it from
// both left and right sides (no deduplication like in ordinary OR tree)
func TestNodeOrAgg_NextSourcedGeq(t *testing.T) {
	left := NewSourcedNodeWrapper(NewStatic([]uint32{1, 2, 3, 5, 8, 15, 19}, false), 0)
	right := NewSourcedNodeWrapper(NewStatic([]uint32{2, 3, 4, 6, 8, 14, 20}, false), 1)

	orAgg := NewNodeOrAgg(left, right, false)

	id, source := orAgg.NextSourcedGeq(NewCmpLIDOrderDesc(3))
	assert.Equal(t, uint32(3), id.Unpack())
	assert.Equal(t, uint32(1), source)

	// 3 returned again, but with different source - no deduplication
	id, source = orAgg.NextSourcedGeq(NewCmpLIDOrderDesc(3))
	assert.Equal(t, uint32(3), id.Unpack())
	assert.Equal(t, uint32(0), source)

	id, source = orAgg.NextSourcedGeq(NewCmpLIDOrderDesc(6))
	assert.Equal(t, uint32(6), id.Unpack())
	assert.Equal(t, uint32(1), source)

	id, source = orAgg.NextSourcedGeq(NewCmpLIDOrderDesc(17))
	assert.Equal(t, uint32(19), id.Unpack())
	assert.Equal(t, uint32(0), source)
}

// TestNodeOrAgg_NextSourcedGeq tests we can navigate to a lid with NextGeq in reverse way and do not skip it from
// both left and right sides (no deduplication like in ordinary OR tree)
func TestNodeOrAgg_NextSourcedGeq_Reverse(t *testing.T) {
	left := NewSourcedNodeWrapper(NewStatic([]uint32{1, 2, 3, 5, 8, 15, 19}, true), 0)
	right := NewSourcedNodeWrapper(NewStatic([]uint32{2, 3, 4, 6, 8, 14, 20}, true), 1)

	orAgg := NewNodeOrAgg(left, right, true)

	id, source := orAgg.NextSourcedGeq(NewCmpLIDOrderAsc(8))
	assert.Equal(t, uint32(8), id.Unpack())
	assert.Equal(t, uint32(1), source)

	// 8 returned again, but with different source - no deduplication
	id, source = orAgg.NextSourcedGeq(NewCmpLIDOrderAsc(8))
	assert.Equal(t, uint32(8), id.Unpack())
	assert.Equal(t, uint32(0), source)

	id, source = orAgg.NextSourcedGeq(NewCmpLIDOrderAsc(4))
	assert.Equal(t, uint32(4), id.Unpack())
	assert.Equal(t, uint32(1), source)

	id, source = orAgg.NextSourcedGeq(NewCmpLIDOrderAsc(1))
	assert.Equal(t, uint32(1), id.Unpack())
	assert.Equal(t, uint32(0), source)

	id, _ = orAgg.NextSourcedGeq(NewCmpLIDOrderAsc(1))
	assert.True(t, id.IsNull())
}

func TestNodeOrAgg_MergeDescending(t *testing.T) {
	left := NewSourcedNodeWrapper(NewStatic([]uint32{1, 3, 5}, true), 0)
	right := NewSourcedNodeWrapper(NewStatic([]uint32{2, 4, 6}, true), 1)

	orAgg := NewNodeOrAgg(left, right, true)
	got := readAllSourced(orAgg)

	want := [][2]uint32{
		{6, 1},
		{5, 0},
		{4, 1},
		{3, 0},
		{2, 1},
		{1, 0},
	}

	assert.Equal(t, want, got)
}

func TestNodeOrAgg_EmptySide(t *testing.T) {
	t.Run("empty_left", func(t *testing.T) {
		left := NewSourcedNodeWrapper(NewStatic(nil, false), 0)
		right := NewSourcedNodeWrapper(NewStatic([]uint32{10, 20}, false), 1)

		orAgg := NewNodeOrAgg(left, right, false)
		got := readAllSourced(orAgg)

		want := [][2]uint32{
			{10, 1},
			{20, 1},
		}

		assert.Equal(t, want, got)
	})

	t.Run("empty_right", func(t *testing.T) {
		left := NewSourcedNodeWrapper(NewStatic([]uint32{10, 20}, false), 0)
		right := NewSourcedNodeWrapper(NewStatic(nil, false), 1)

		orAgg := NewNodeOrAgg(left, right, false)
		got := readAllSourced(orAgg)

		want := [][2]uint32{
			{10, 0},
			{20, 0},
		}

		assert.Equal(t, want, got)
	})

	t.Run("both_empty", func(t *testing.T) {
		left := NewSourcedNodeWrapper(NewStatic(nil, false), 0)
		right := NewSourcedNodeWrapper(NewStatic(nil, false), 1)

		orAgg := NewNodeOrAgg(left, right, false)
		id, _ := orAgg.NextSourced()

		assert.True(t, id.IsNull())
	})
}

func readAllSourced(n Sourced) [][2]uint32 {
	var res [][2]uint32
	id, src := n.NextSourced()
	for !id.IsNull() {
		res = append(res, [2]uint32{id.Unpack(), src})
		id, src = n.NextSourced()
	}
	return res
}
