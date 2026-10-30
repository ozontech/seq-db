package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
)

func TestNodeRange_NextGeq_JumpToLast(t *testing.T) {
	node := NewRange(NewAscLID(3), NewAscLID(10))

	id := node.NextGeq(NewAscLID(10))
	assert.Equal(t, uint32(10), id.Unpack())
}

func TestNodeRange_NextGeq_SkipsWholeRange(t *testing.T) {
	node := NewRange(NewAscLID(3), NewAscLID(10))

	id := node.NextGeq(NewAscLID(11))
	assert.True(t, id.IsNull())
}

func TestNodeRange_NextGeq(t *testing.T) {
	node := NewRange(NewAscLID(3), NewAscLID(10))

	id := node.NextGeq(NewAscLID(5))
	assert.Equal(t, uint32(5), id.Unpack())

	id = node.NextGeq(NewAscLID(5))
	assert.Equal(t, uint32(6), id.Unpack())

	id = node.NextGeq(NewAscLID(10))
	assert.Equal(t, uint32(10), id.Unpack())

	id = node.NextGeq(NewAscLID(10))
	assert.True(t, id.IsNull())
}

func TestNodeRange_NextGeq_Reverse(t *testing.T) {
	node := NewRange(NewDescLID(10), NewDescLID(3))

	id := node.NextGeq(NewDescLID(9))
	assert.Equal(t, uint32(9), id.Unpack())

	id = node.NextGeq(NewDescLID(9))
	assert.Equal(t, uint32(8), id.Unpack())

	id = node.NextGeq(NewDescLID(4))
	assert.Equal(t, uint32(4), id.Unpack())

	id = node.NextGeq(NewDescLID(3))
	assert.Equal(t, uint32(3), id.Unpack())

	id = node.NextGeq(NewDescLID(3))
	assert.True(t, id.IsNull())
}

func collectBatched(t *testing.T, n BatchedNode) []uint32 {
	t.Helper()
	var got []uint32
	for {
		b := n.NextBatch()
		if b.IsEmpty() {
			return got
		}
		it := b.Iter()
		for v, ok := it.Next(); ok; v, ok = it.Next() {
			got = append(got, v)
		}
	}
}

func rangeLIDs(from, to int) []uint32 {
	var out []uint32
	for v := from; v <= to; v++ {
		out = append(out, uint32(v))
	}
	return out
}

func TestRangeBatched(t *testing.T) {
	tests := []struct {
		name     string
		min, max uint32
		asc      bool
		want     []uint32
	}{
		{name: "asc", min: 3, max: 10, asc: true, want: rangeLIDs(3, 10)},
		{name: "desc", min: 3, max: 10, want: rangeLIDs(3, 10)},
		{name: "empty asc", min: 10, max: 3, asc: true},
		{name: "empty desc", min: 10, max: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectBatched(t, NewRangeBatched(tt.min, tt.max, tt.asc))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRangeBatchedNextGeq(t *testing.T) {
	tests := []struct {
		name             string
		asc              bool
		next             LID
		wantMin, wantMax uint32
	}{
		{name: "asc", asc: true, next: NewAscLID(5), wantMin: 5, wantMax: 10},
		{name: "desc", next: NewDescLID(7), wantMin: 3, wantMax: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewRangeBatched(3, 10, tt.asc).NextBatchGeq(tt.next)
			require.False(t, b.IsEmpty())
			require.Equal(t, tt.wantMin, b.Min())
			require.Equal(t, tt.wantMax, b.Max())
		})
	}
}

func TestRangeBatchedWholeWindowInOneBatch(t *testing.T) {
	n := NewRangeBatched(1, uint32(consts.DefaultLIDBlockCap)+10, true)
	b := n.NextBatch()
	require.False(t, b.IsEmpty())
	require.Equal(t, consts.DefaultLIDBlockCap+10, b.Len())
	require.True(t, n.NextBatch().IsEmpty())
}

func TestRangeBatchedUint32Bounds(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
		asc   bool
	}{
		{name: "max asc", value: math.MaxUint32, asc: true},
		{name: "zero desc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewRangeBatched(tt.value, tt.value, tt.asc)
			b := n.NextBatch()
			it := b.ManyIter(tt.asc)
			buf := make([]uint32, 1)
			require.Equal(t, 1, it.CopyRawLIDs(buf))
			require.Equal(t, tt.value, buf[0])
			require.Zero(t, it.CopyRawLIDs(buf))
			require.True(t, n.NextBatch().IsEmpty())
		})
	}
}

func TestRangeBatchFullUint32Range(t *testing.T) {
	b := newBitmapBatchFromRange(0, math.MaxUint32)
	require.Equal(t, uint64(math.MaxUint32)+1, uint64(b.Len()))

	buf := make([]uint32, 1)
	require.Equal(t, 1, b.ManyIter(true).CopyRawLIDs(buf))
	require.Equal(t, uint32(0), buf[0])
	require.Equal(t, 1, b.ManyIter(false).CopyRawLIDs(buf))
	require.Equal(t, uint32(math.MaxUint32), buf[0])
}
