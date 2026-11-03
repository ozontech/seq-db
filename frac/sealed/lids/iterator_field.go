package lids

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

// FieldIterator allows to batch-scroll through all LID lists for a particular field.
type FieldIterator struct {
	table        *Table
	loader       *Loader
	counter      Counter
	firstTID     uint32 // inclusive
	lastTID      uint32 // inclusive
	nextBlockIdx uint32 // next (not yet processed) block id to load LIDs from
	lastBlockIdx uint32 // index of the last block which has LIDs for this field
	done         bool
}

func NewFieldIterator(
	table *Table,
	loader *Loader,
	firstTID, lastTID uint32,
	counter Counter,
) *FieldIterator {
	return &FieldIterator{
		table:        table,
		loader:       loader,
		counter:      counter,
		firstTID:     firstTID,
		lastTID:      lastTID,
		nextBlockIdx: table.GetFirstBlockIndexForTID(firstTID),
		lastBlockIdx: table.GetLastBlockIndexForTID(lastTID),
	}
}

// NextBatch returns lids and offsets for the next LID block.
// isFirstLID is true when the first posting list in the batch begins in this block
// (as opposed to continuing a list split across the previous block).
// Exhausted when len(lids) == 0.
func (c *FieldIterator) NextBatch(lids, offsets []uint32) ([]uint32, []uint32, bool) {
	block, blockIdx, blockMinTID, firstListIdx, lastListIdx := c.loadNextBlock()
	if block == nil {
		return nil, nil, false
	}

	lids = lids[:0]
	offsets = offsets[:0]
	if block.IsDeltaEncoded() {
		lids, offsets = copyDeltaBlock(block, firstListIdx, lastListIdx, lids, offsets)
	} else {
		lids, offsets = copyHybridBlock(block, firstListIdx, lastListIdx, lids, offsets)
	}

	if c.counter != nil {
		c.counter.AddLIDsCount(len(lids))
	}

	firstTIDInBatch := blockMinTID + uint32(firstListIdx)
	isFirstLID := !c.table.HasTIDInPrevBlock(blockIdx, firstTIDInBatch)
	return lids, offsets, isFirstLID
}

func (c *FieldIterator) loadNextBlock() (block *Block, blockIdx, blockMinTID uint32, firstListIdx, lastListIdx int) {
	if c.done {
		return nil, 0, 0, 0, 0
	}

	if c.nextBlockIdx > c.lastBlockIdx {
		c.done = true
		return nil, 0, 0, 0, 0
	}

	blockIdx = c.nextBlockIdx
	c.nextBlockIdx++

	var err error
	block, err = c.loader.GetLIDsBlock(c.table.StartBlockIndex + blockIdx)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	numLists := int(c.table.GetChunksCount(blockIdx))
	blockMinTID = c.table.GetAdjustedMinTID(blockIdx)
	// find LID list indexes within current block where [firstTID,lastTID] interval overlaps with the block
	firstListIdx = min(numLists, max(0, int(c.firstTID)-int(blockMinTID)))
	lastListIdx = min(numLists, int(c.lastTID)-int(blockMinTID)+1)

	if firstListIdx < lastListIdx {
		return block, blockIdx, blockMinTID, firstListIdx, lastListIdx
	}

	return nil, 0, 0, 0, 0
}

// copyDeltaBlock fills the batch for delta-encoded block. Copies entire offsets and lids from LIDs block,
// so it's faster than copyHybridBlock.
func copyDeltaBlock(
	block *Block,
	firstListIdx, lastListIdx int,
	lids, offsets []uint32,
) ([]uint32, []uint32) {
	numLists := lastListIdx - firstListIdx
	firstOffset := block.offsets[firstListIdx]
	numLIDs := int(block.offsets[lastListIdx] - block.offsets[firstListIdx])

	lids = util.EnsureSliceSize(lids, numLIDs)
	copy(lids, block.lids[block.offsets[firstListIdx]:block.offsets[lastListIdx]])

	offsets = util.EnsureSliceSize(offsets, numLists+1)
	copy(offsets, block.offsets[firstListIdx:lastListIdx+1])

	// adjust offsets if copied not from the beginning of the block
	if firstListIdx != 0 {
		for i := range offsets {
			offsets[i] -= firstOffset
		}
	}

	return lids, offsets
}

// copyHybridBlock copies lids from the block list-by-list.
func copyHybridBlock(
	block *Block,
	firstListIdx, lastListIdx int,
	lids, offsets []uint32,
) ([]uint32, []uint32) {
	offsets = append(offsets, 0)

	for idx := firstListIdx; idx < lastListIdx; idx++ {
		lids, _ = block.AppendLIDsTo(idx, lids)
		offsets = append(offsets, uint32(len(lids)))
	}
	return lids, offsets
}
