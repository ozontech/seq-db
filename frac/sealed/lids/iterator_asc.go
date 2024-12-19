package lids

import (
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type IteratorAsc Cursor

func (*IteratorAsc) String() string {
	return "LIDS_ASC"
}

// narrowLIDsRange cuts LIDs between from and to. Returns new lids and tryNextBlock flag
func (it *IteratorAsc) narrowLIDsRange(lids []uint32, tryNextBlock bool) ([]uint32, bool) {
	first := lids[0]
	if it.maxLID < first { // fast path: out-of-bounds 1; allowed to continue reading blocks
		return nil, tryNextBlock
	}

	last := lids[len(lids)-1]
	if it.minLID > last { // fast path: out-of-bounds 2
		return nil, false // stop reading blocks
	}

	if it.minLID > first {
		left := sort.Search(len(lids), func(i int) bool { return lids[i] >= it.minLID })
		lids = lids[left:]
		tryNextBlock = false
	}

	if it.maxLID <= last {
		right := sort.Search(len(lids), func(i int) bool { return lids[i] > it.maxLID })
		lids = lids[:right]
	}

	return lids, tryNextBlock
}

func (it *IteratorAsc) loadNextLIDsChunk() {
	chunks, err := it.loader.GetLIDsChunks(it.table.StartIndex + it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	if chunks.getCount() != int(it.table.GetChunksCount(it.blockIndex)) {
		logger.Panic("unexpected LIDs count")
	}

	it.lids = chunks.getLIDs(it.table.GetChunkIndex(it.blockIndex, it.tid))
	it.tryNextBlock = it.table.HasTIDInPrevBlock(it.blockIndex, it.tid)
	it.blockIndex--
}

func (it *IteratorAsc) Next() (uint32, bool) {
	for len(it.lids) == 0 {
		if !it.tryNextBlock {
			return 0, false
		}

		it.loadNextLIDsChunk() // last chunk in block but not last for tid; need load next block
		it.lids, it.tryNextBlock = it.narrowLIDsRange(it.lids, it.tryNextBlock)
		it.counter.AddLIDsCount(len(it.lids)) // inc loaded LIDs count
	}

	i := len(it.lids) - 1
	lid := it.lids[i]
	it.lids = it.lids[:i]
	return lid, true
}
