package lids

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

// FieldIterator allows to batch-scroll through all LID lists for a particular field.
type FieldIterator struct {
	table        *Table
	loader       *Loader
	counter      Counter
	firstTID     uint32 // inclusive
	lastTID      uint32 // inclusive
	blockIdx     uint32
	lastBlockIdx uint32
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
		blockIdx:     table.GetFirstBlockIndexForTID(firstTID),
		lastBlockIdx: table.GetLastBlockIndexForTID(lastTID),
	}
}

// NextBatch returns lids and offsets for the next LID block.
// isFirstLID is true when the first posting list in the batch begins in this block
// (as opposed to continuing a list split across the previous block).
// Exhausted when len(offsets) < 2.
func (c *FieldIterator) NextBatch(lids, offsets []uint32) ([]uint32, []uint32, bool) {
	for {
		block, blockIdx, blockMinTID, firstListIdx, lastListIdx, ok := c.loadNextBlock()
		if !ok {
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
}

func (c *FieldIterator) loadNextBlock() (block *Block, blockIdx, blockMinTID uint32, firstListIdx, lastListIdx int, ok bool) {
	for !c.done {
		if c.blockIdx > c.lastBlockIdx {
			c.done = true
			return nil, 0, 0, 0, 0, false
		}

		blockIdx = c.blockIdx
		c.blockIdx++

		var err error
		block, err = c.loader.GetLIDsBlock(c.table.StartBlockIndex + blockIdx)
		if err != nil {
			logger.Panic("error loading LIDs block", zap.Error(err))
		}

		numLists := int(c.table.GetChunksCount(blockIdx))
		if block.GetCount() != numLists {
			logger.Panic("unexpected LIDs count")
		}

		blockMinTID = c.table.GetAdjustedMinTID(blockIdx)
		firstListIdx = 0
		if blockMinTID < c.firstTID {
			firstListIdx = int(c.firstTID - blockMinTID)
			if firstListIdx > numLists {
				firstListIdx = numLists
			}
		}
		lastListIdx = min(numLists, int(c.lastTID-blockMinTID+1))

		if firstListIdx < lastListIdx {
			return block, blockIdx, blockMinTID, firstListIdx, lastListIdx, true
		}
	}
	return nil, 0, 0, 0, 0, false
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

	lids = ensureCap(lids, numLIDs)
	copy(lids, block.lids[block.offsets[firstListIdx]:block.offsets[lastListIdx]])

	offsets = ensureCap(offsets, numLists+1)

	if firstListIdx == 0 {
		copy(offsets, block.offsets[:lastListIdx+1])
	} else {
		srcOff := block.offsets[firstListIdx : lastListIdx+1]
		for i := 0; i <= numLists; i++ {
			offsets[i] = srcOff[i] - firstOffset
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
		var n int
		lids, n = block.AppendLIDsTo(idx, lids)
		if n == 0 {
			continue
		}
		offsets = append(offsets, uint32(len(lids)))
	}
	return lids, offsets
}

func ensureCap(s []uint32, n int) []uint32 {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]uint32, n)
}
