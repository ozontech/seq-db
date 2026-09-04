package lids

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
)

// BatchedIteratorAsc iterates LID batches in ascending order (low to high).
type BatchedIteratorAsc struct {
	Cursor
}

func NewBatchedIteratorAsc(it *IteratorAsc) *BatchedIteratorAsc {
	return &BatchedIteratorAsc{
		Cursor: it.Cursor,
	}
}

func (*BatchedIteratorAsc) String() string {
	return "LIDS_ASC_BATCHED"
}

// narrowLIDsRange cuts LIDs between minLID and maxLID. Returns updated tryNextBlock flag.
func (it *BatchedIteratorAsc) narrowLIDsRange(tryNextBlock bool) bool {
	if it.batch.IsEmpty() {
		return tryNextBlock
	}

	first := it.batch.Min()
	if it.maxLID < first {
		it.batch = node.EmptyBatch()
		return false
	}

	batchMax := it.batch.Max()
	if it.minLID > batchMax {
		it.batch = node.EmptyBatch()
		return tryNextBlock
	}

	lastBlock := it.maxLID < batchMax
	it.batch = it.batch.Narrow(it.minLID, it.maxLID)
	if lastBlock {
		tryNextBlock = false
	}

	return tryNextBlock
}

func (it *BatchedIteratorAsc) loadNextLIDsBlock() {
	block, err := it.loader.GetLIDsBlock(it.table.StartBlockIndex + it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	if block.GetCount() != int(it.table.GetChunksCount(it.blockIndex)) {
		logger.Panic("unexpected LIDs count")
	}

	it.batch = block.GetLIDs(it.table.GetChunkIndex(it.blockIndex, it.tid))
	it.tryNextBlock = it.table.HasTIDInNextBlock(it.blockIndex, it.tid)
	it.tryNextBlock = it.narrowLIDsRange(it.tryNextBlock)
	it.counter.AddLIDsCount(it.batch.Len())
	it.blockIndex++
}

func (it *BatchedIteratorAsc) NextBatch() node.LIDBatch {
	return it.NextBatchGeq(node.NewAscZeroLID())
}

func (it *BatchedIteratorAsc) NextBatchGeq(nextID node.LID) node.LIDBatch {
	for {
		if it.batch.IsEmpty() {
			if !it.tryNextBlock {
				return node.EmptyBatch()
			}

			it.blockIndex = it.table.SeekBlockGeq(it.blockIndex, it.tid, nextID.Unpack())
			it.loadNextLIDsBlock()
		}

		if it.batch.IsEmpty() {
			continue
		}

		if nextID.Unpack() > it.batch.Max() {
			it.batch = node.EmptyBatch()
			continue
		}

		out := it.batch
		it.batch = node.EmptyBatch()
		return out
	}
}
