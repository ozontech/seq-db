package lids

import (
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type IteratorDesc Cursor

func (*IteratorDesc) String() string {
	return "LIDS_DESC"
}

// narrowLIDsRange cuts LIDs between from and to. Returns new lids and tryNextBlock flag
func (it *IteratorDesc) narrowLIDsRange(lids []uint32, tryNextBlock bool) ([]uint32, bool) {
	first := lids[0]
	if it.maxLID < first { // fast path: out-of-bounds 1
		return nil, false // stop reading blocks
	}

	last := lids[len(lids)-1]
	if it.minLID > last { // fast path: out-of-bounds 2; allowed to continue reading blocks
		return nil, tryNextBlock
	}

	if it.minLID > first {
		left := sort.Search(len(lids), func(i int) bool { return lids[i] >= it.minLID })
		lids = lids[left:]
	}

	if it.maxLID <= last {
		right := sort.Search(len(lids), func(i int) bool { return lids[i] > it.maxLID })
		lids = lids[:right]
		tryNextBlock = false
	}

	return lids, tryNextBlock
}

func (it *IteratorDesc) loadNextLIDsBlock() {
	block, err := it.loader.GetLIDsBlock(it.table.StartBlockIndex + it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	if block.getCount() != int(it.table.GetChunksCount(it.blockIndex)) {
		logger.Panic("unexpected LIDs count")
	}

	it.lids = block.getLIDs(it.table.GetChunkIndex(it.blockIndex, it.tid))
	it.tryNextBlock = it.table.HasTIDInNextBlock(it.blockIndex, it.tid)
	it.blockIndex++
}

func (it *IteratorDesc) Next() (uint32, bool) {
	for len(it.lids) == 0 {
		if !it.tryNextBlock {
			return 0, false
		}

		it.loadNextLIDsBlock() // last chunk in block but not last for tid; need load next block
		it.lids, it.tryNextBlock = it.narrowLIDsRange(it.lids, it.tryNextBlock)
		it.counter.AddLIDsCount(len(it.lids)) // inc loaded LIDs count
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return lid, true
}

func (it *IteratorDesc) NextGeq(minLID uint32) (uint32, bool) {
	for {
		for len(it.lids) == 0 {
			if !it.tryNextBlock {
				return 0, false
			}

			it.loadNextLIDsBlock() // last chunk in block but not last for tid; need load next block
			it.lids, it.tryNextBlock = it.narrowLIDsRange(it.lids, it.tryNextBlock)
			it.counter.AddLIDsCount(len(it.lids)) // inc loaded LIDs count
		}

		last := it.lids[len(it.lids)-1]
		// fast path check: the last LID is less than minLID. if true, then skip the entire block
		if minLID > last {
			it.lids = it.lids[:0]
			continue
		}

		l := len(it.lids)

		if l >= 32 && it.lids[31] > minLID {
			idx := sort.Search(len(it.lids[0:32]), func(i int) bool { return it.lids[i] >= minLID })
			if idx < 32 {
				// TODO single it.lids = it.lids
				it.lids = it.lids[idx:]
				lid := it.lids[0]
				it.lids = it.lids[1:]
				return lid, true
			}

			if len(it.lids) == 0 {
				continue
			}
		}

		// use binary search to find lower bound
		idx := sort.Search(len(it.lids), func(i int) bool { return it.lids[i] >= minLID })
		if idx < len(it.lids) {
			it.lids = it.lids[idx:]
			lid := it.lids[0]
			it.lids = it.lids[1:]
			return lid, true
		}

		it.lids = it.lids[:0]
	}
}
