package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeBatchManyIter(t *testing.T) {
	tests := []struct {
		name string
		asc  bool
		want []uint32
	}{
		{name: "asc", asc: true, want: []uint32{3, 4, 5, 6, 7}},
		{name: "desc", want: []uint32{7, 6, 5, 4, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := newRangeBatch(3, 7).ManyIter(tt.asc)
			buf := make([]uint32, 2)
			var got []uint32
			for {
				n := it.CopyRawLIDs(buf)
				if n == 0 {
					break
				}
				got = append(got, buf[:n]...)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRangeBatchCopyLIDsTmpLimit(t *testing.T) {
	it := newRangeBatch(3, 7).ManyIter(true)
	dst := make([]LID, 4)
	tmp := make([]uint32, 2)

	n := it.CopyLIDs(dst, tmp)
	require.Equal(t, 2, n)
	require.Equal(t, []uint32{3, 4}, []uint32{dst[0].Unpack(), dst[1].Unpack()})

	n = it.CopyLIDs(dst, tmp)
	require.Equal(t, 2, n)
	require.Equal(t, []uint32{5, 6}, []uint32{dst[0].Unpack(), dst[1].Unpack()})

	n = it.CopyLIDs(dst, tmp)
	require.Equal(t, 1, n)
	require.Equal(t, uint32(7), dst[0].Unpack())
	require.Zero(t, it.CopyLIDs(dst, tmp))
}

func TestRangeBatchNarrow(t *testing.T) {
	tests := []struct {
		name   string
		minLID uint32
		maxLID uint32
		want   []uint32
	}{
		{name: "full", minLID: 1, maxLID: 12, want: []uint32{3, 4, 5, 6, 7, 8, 9, 10}},
		{name: "partial", minLID: 5, maxLID: 8, want: []uint32{5, 6, 7, 8}},
		{name: "empty before", minLID: 1, maxLID: 2},
		{name: "empty after", minLID: 11, maxLID: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newRangeBatch(3, 10).Narrow(tt.minLID, tt.maxLID)
			require.Equal(t, tt.want, toSlice(got))
		})
	}
}

func TestRangeBatchedOutOfBoundsExhaustsNode(t *testing.T) {
	tests := []struct {
		name   string
		asc    bool
		nextID LID
	}{
		{name: "asc", asc: true, nextID: NewAscLID(11)},
		{name: "desc", nextID: NewDescLID(2)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewRangeBatched(3, 10, tt.asc)
			require.True(t, n.NextBatchGeq(tt.nextID).IsEmpty())
			require.True(t, n.NextBatch().IsEmpty())
		})
	}
}
