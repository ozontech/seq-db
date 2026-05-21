package lids

import (
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
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

func (it *IteratorAsc) loadNextLIDsBlock() {
	block, err := it.loader.GetLIDsBlock(it.table.StartBlockIndex + it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	if block.getCount() != int(it.table.GetChunksCount(it.blockIndex)) {
		logger.Panic("unexpected LIDs count")
	}

	it.lids = block.getLIDs(it.table.GetChunkIndex(it.blockIndex, it.tid))
	it.tryNextBlock = it.table.HasTIDInPrevBlock(it.blockIndex, it.tid)
	it.blockIndex--
}

func (it *IteratorAsc) Next() node.LID {
	for len(it.lids) == 0 {
		if !it.tryNextBlock {
			return node.NullLID()
		}

		it.loadNextLIDsBlock() // last chunk in block but not last for tid; need load next block
		it.lids, it.tryNextBlock = it.narrowLIDsRange(it.lids, it.tryNextBlock)
		it.counter.AddLIDsCount(len(it.lids)) // inc loaded LIDs count
	}

	i := len(it.lids) - 1
	lid := it.lids[i]
	it.lids = it.lids[:i]
	fmt.Printf("lid: %v\n", lid)

	return node.NewAscLID(lid)
}

// NextGeq returns the next (in reverse iteration order) LID that is <= maxLID.
func (it *IteratorAsc) NextGeq(nextID node.LID) node.LID {
	for {
		for len(it.lids) == 0 {
			if !it.tryNextBlock {
				return node.NullLID()
			}

			it.loadNextLIDsBlock()
			it.lids, it.tryNextBlock = it.narrowLIDsRange(it.lids, it.tryNextBlock)
			it.counter.AddLIDsCount(len(it.lids))
		}

		// fast path: smallest remaining > nextID => skip entire block
		// TODO(cheb0): We could also pass LID into narrowLIDsRange to perform block skipping once we add something like MinLID to LID block header
		if it.lids[0] > nextID.Unpack() {
			it.lids = it.lids[:0]
			continue
		}

		idx := sort.Search(len(it.lids), func(i int) bool { return it.lids[i] > nextID.Unpack() }) - 1
		if idx >= 0 {
			lid := it.lids[idx]
			it.lids = it.lids[:idx]
			return node.NewAscLID(lid)
		}

		it.lids = it.lids[:0]
	}
}
