package docsfilter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergedIterator(t *testing.T) {
	iterators := []TombstonesIterator{
		&testIterator{lids: []uint32{1, 2, 5, 22, 45}},
		&testIterator{lids: []uint32{2, 3, 9, 15, 33, 45}},
		&testIterator{lids: []uint32{1, 7, 8, 45}},
	}

	mergedIterator := NewNMergedIterators(iterators)
	resLIDs := make([]uint32, 0)
	for {
		lid, has := mergedIterator.Next()
		if !has {
			break
		}
		resLIDs = append(resLIDs, lid)

	}
	require.Equal(t, []uint32{1, 2, 3, 5, 7, 8, 9, 15, 22, 33, 45}, resLIDs)
}

type testIterator struct {
	lids []uint32
}

func (it *testIterator) Next() (uint32, bool) {
	if len(it.lids) == 0 {
		return 0, false
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return lid, true
}
