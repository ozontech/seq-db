package skipmaskmanager

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/node"
)

func TestMergedIterator(t *testing.T) {
	iterators := []node.Node{
		&testIteratorAsc{lids: []uint32{1, 2, 5, 22, 45}},
		&testIteratorAsc{lids: []uint32{2, 3, 9, 15, 33, 45}},
		&testIteratorAsc{lids: []uint32{1, 7, 8, 45}},
	}

	mergedIterator := NewNMergedIterators(iterators)
	resLIDs := make([]uint32, 0)
	for {
		lid := mergedIterator.Next()
		if lid.IsNull() {
			break
		}
		resLIDs = append(resLIDs, lid.Unpack())

	}
	require.Equal(t, []uint32{1, 2, 3, 5, 7, 8, 9, 15, 22, 33, 45}, resLIDs)
}

func TestMergedIteratorReverse(t *testing.T) {
	iterators := []node.Node{
		&testIteratorDesc{lids: []uint32{45, 22, 5, 2, 1}},
		&testIteratorDesc{lids: []uint32{45, 33, 15, 9, 3, 2}},
		&testIteratorDesc{lids: []uint32{45, 8, 7, 1}},
	}

	mergedIterator := NewNMergedIterators(iterators)
	resLIDs := make([]uint32, 0)
	for {
		lid := mergedIterator.Next()
		if lid.IsNull() {
			break
		}
		resLIDs = append(resLIDs, lid.Unpack())

	}
	require.Equal(t, []uint32{45, 33, 22, 15, 9, 8, 7, 5, 3, 2, 1}, resLIDs)
}

type testIteratorAsc struct {
	lids []uint32
}

func (it *testIteratorAsc) String() string {
	return "TEST_SKIP_MASK_ITERATOR_ASC"
}

func (it *testIteratorAsc) Next() node.LID {
	if len(it.lids) == 0 {
		return node.NullLID()
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return node.NewAscLID(lid)
}

func (it *testIteratorAsc) NextGeq(nextID node.LID) node.LID {
	return node.NullLID()
}

type testIteratorDesc struct {
	lids []uint32
}

func (it *testIteratorDesc) String() string {
	return "TEST_SKIP_MASK_ITERATOR_DESC"
}

func (it *testIteratorDesc) Next() node.LID {
	if len(it.lids) == 0 {
		return node.NullLID()
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return node.NewDescLID(lid)
}

func (it *testIteratorDesc) NextGeq(nextID node.LID) node.LID {
	return node.NullLID()
}
