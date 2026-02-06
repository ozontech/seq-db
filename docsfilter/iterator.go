package docsfilter

import "sort"

type Iterator struct {
	loader *loader

	minLID uint32
	maxLID uint32

	blockIndex   int
	tryNextBlock bool

	lids []uint32
}

func NewIterator(
	loader *loader,
	minLID uint32,
	maxLID uint32,
) *Iterator {
	return &Iterator{
		loader:       loader,
		minLID:       minLID,
		maxLID:       maxLID,
		tryNextBlock: true,
	}
}

func (it *Iterator) hasLIDsInRange() bool {
	if it.loader.headers[it.blockIndex].MinLID > it.maxLID {
		return false
	}
	if it.loader.headers[it.blockIndex].MaxLID < it.minLID {
		return false
	}

	return true
}

// narrowLIDsRange cuts LIDs between from and to. Returns new lids
func (it *Iterator) narrowLIDsRange(lids []uint32) []uint32 {
	if len(lids) == 0 {
		return lids
	}

	first := lids[0]
	last := lids[len(lids)-1]

	if it.minLID > first {
		left := sort.Search(len(lids), func(i int) bool { return lids[i] >= it.minLID })
		lids = lids[left:]
	}

	if it.maxLID <= last {
		right := sort.Search(len(lids), func(i int) bool { return lids[i] > it.maxLID })
		lids = lids[:right]
	}

	return lids
}
