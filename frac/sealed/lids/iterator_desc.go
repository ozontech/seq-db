package lids

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
)

type IteratorDesc struct {
	Cursor
	it node.Iter
}

func NewIteratorDesc(
	table *Table,
	loader *Loader,
	startIndex uint32,
	tid uint32,
	counter Counter,
	minLID, maxLID uint32,
) *IteratorDesc {
	it := &IteratorDesc{
		Cursor: *NewLIDsCursor(table, loader, startIndex, tid, counter, minLID, maxLID),
	}
	it.it = it.batch.Iter()
	return it
}

func (*IteratorDesc) String() string {
	return "LIDS_DESC"
}

// narrowLIDsRange cuts LIDs between minLID and maxLID. Returns updated tryNextBlock flag.
func (it *IteratorDesc) narrowLIDsRange(tryNextBlock bool) bool {
	if it.batch.IsEmpty() {
		return tryNextBlock
	}

	first := it.batch.Min()
	if it.maxLID < first {
		it.batch = node.EmptyBatch()
		return false
	}

	last := it.batch.Max()
	if it.minLID > last {
		it.batch = node.EmptyBatch()
		return tryNextBlock
	}

	lastBlock := it.maxLID < last
	it.batch = it.batch.Narrow(it.minLID, it.maxLID)
	if lastBlock {
		tryNextBlock = false
	}

	return tryNextBlock
}

func (it *IteratorDesc) loadNextLIDsBlock() {
	block, err := it.loader.GetLIDsBlock(it.table.StartBlockIndex + it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	if block.GetCount() != int(it.table.GetChunksCount(it.blockIndex)) {
		logger.Panic("unexpected LIDs count")
	}

	it.batch = block.GetLIDs(it.table.GetChunkIndex(it.blockIndex, it.tid))
	it.counter.AddLIDsCount(it.batch.Len())
	tryNextBlock := it.table.HasTIDInNextBlock(it.blockIndex, it.tid)
	it.tryNextBlock = it.narrowLIDsRange(tryNextBlock)
	it.it = it.batch.Iter()
	it.blockIndex++
}

func (it *IteratorDesc) Next() node.LID {
	for {
		v, ok := it.it.Next()
		if ok {
			return node.NewDescLID(v)
		}
		if !it.tryNextBlock {
			return node.NullLID()
		}
		it.loadNextLIDsBlock()
	}
}

// NextGeq finds next greater or equal
func (it *IteratorDesc) NextGeq(nextID node.LID) node.LID {
	for {
		v, ok := it.it.NextGeq(nextID.Unpack())
		if ok {
			return node.NewDescLID(v)
		}
		if !it.tryNextBlock {
			return node.NullLID()
		}
		// TODO it.blockIndex = it.table.SeekBlockGeq(it.blockIndex, it.tid, nextID.Unpack())
		it.loadNextLIDsBlock()
	}
}
