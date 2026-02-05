package docsfilter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/node"
)

func TestMergedIterator(t *testing.T) {
	iterators := []node.Node{
		&testIterator{lids: []uint32{1, 2, 5, 22, 45}},
		&testIterator{lids: []uint32{2, 3, 9, 15, 33, 45}},
		&testIterator{lids: []uint32{1, 7, 8, 45}},
	}

	mergedIterator := NewNMergedIterators(iterators, false)
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

func TestMergedIteratorReverse(t *testing.T) {
	iterators := []node.Node{
		&testIterator{lids: []uint32{45, 22, 5, 2, 1}},
		&testIterator{lids: []uint32{45, 33, 15, 9, 3, 2}},
		&testIterator{lids: []uint32{45, 8, 7, 1}},
	}

	mergedIterator := NewNMergedIterators(iterators, true)
	resLIDs := make([]uint32, 0)
	for {
		lid, has := mergedIterator.Next()
		if !has {
			break
		}
		resLIDs = append(resLIDs, lid)

	}
	require.Equal(t, []uint32{45, 33, 22, 15, 9, 8, 7, 5, 3, 2, 1}, resLIDs)
}

type testIterator struct {
	lids []uint32
}

func (it *testIterator) String() string {
	return "TEST_TOMBSTONES_ITERATOR"
}

func (it *testIterator) Next() (uint32, bool) {
	if len(it.lids) == 0 {
		return 0, false
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return lid, true
}
