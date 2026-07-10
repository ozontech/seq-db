package lids

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
)

type IteratorAsc struct {
	Cursor
	it node.Iter
}

func NewIteratorAsc(
	table *Table,
	loader *Loader,
	startIndex uint32,
	tid uint32,
	counter Counter,
	minLID, maxLID uint32,
) *IteratorAsc {
	it := &IteratorAsc{
		Cursor: *NewLIDsCursor(table, loader, startIndex, tid, counter, minLID, maxLID),
	}
	it.it = it.batch.ReverseIter()
	return it
}

func (*IteratorAsc) String() string {
	return "LIDS_ASC"
}

// narrowLIDsRange cuts LIDs between minLID and maxLID. Returns updated tryNextBlock flag.
func (it *IteratorAsc) narrowLIDsRange(tryNextBlock bool) bool {
	if it.batch.IsEmpty() {
		return tryNextBlock
	}

	first := it.batch.Min()
	if it.maxLID < first {
		it.batch = node.EmptyBatch()
		return tryNextBlock
	}

	last := it.batch.Max()
	if it.minLID > last {
		it.batch = node.EmptyBatch()
		return false
	}

	lastBlock := it.minLID > first
	it.batch = it.batch.Narrow(it.minLID, it.maxLID)
	if lastBlock {
		tryNextBlock = false
	}

	return tryNextBlock
}

func (it *IteratorAsc) loadNextLIDsBlock() {
	block, err := it.loader.GetLIDsBlock(it.table.StartBlockIndex + it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	if block.GetCount() != int(it.table.GetChunksCount(it.blockIndex)) {
		logger.Panic("unexpected LIDs count")
	}

	it.batch = block.GetLIDs(it.table.GetChunkIndex(it.blockIndex, it.tid))
	tryNextBlock := it.table.HasTIDInPrevBlock(it.blockIndex, it.tid)
	it.tryNextBlock = it.narrowLIDsRange(tryNextBlock)
	it.it = it.batch.ReverseIter()
	it.counter.AddLIDsCount(it.batch.Len())
	it.blockIndex--
}

func (it *IteratorAsc) Next() node.LID {
	for {
		lid, ok := it.it.Next()
		if ok {
			return node.NewAscLID(lid)
		}
		if !it.tryNextBlock {
			return node.NullLID()
		}
		it.loadNextLIDsBlock()
	}
}

// NextGeq returns the next (in reverse iteration order) LID that is <= maxLID.
func (it *IteratorAsc) NextGeq(nextID node.LID) node.LID {
	for {
		lid, ok := it.it.NextGeq(nextID.Unpack())
		if ok {
			return node.NewAscLID(lid)
		}
		if !it.tryNextBlock {
			return node.NullLID()
		}
		// TODO it.blockIndex = it.table.SeekBlockLeq(it.blockIndex, it.tid, nextID.Unpack())
		it.loadNextLIDsBlock()
	}
}
