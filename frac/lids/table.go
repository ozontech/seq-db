package lids

import (
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type Table struct {
	StartIndex uint32
	MaxTIDs    []uint32 // defines last tid for each block
	MinTIDs    []uint32 // defines first not continued tid for each block

	// TODO: We need fix MinTID issue that we have to compensate with DiskBlock.getAdjustedMinTID()
	// TODO: After that we do not need store IsContinued flag, and able calc it as MaxTIDs[i] == MinTIDs[i+1]
	IsContinued []bool
}

func NewTable(startOfLIDsBlockIndex uint32, minTIDs, maxTIDs []uint32, isContinued []bool) *Table {
	return &Table{
		StartIndex:  startOfLIDsBlockIndex,
		MinTIDs:     minTIDs,
		MaxTIDs:     maxTIDs,
		IsContinued: isContinued,
	}
}

func (t *Table) Add(block *Block) {
	t.MinTIDs = append(t.MinTIDs, block.MinTID)
	t.MaxTIDs = append(t.MaxTIDs, block.MaxTID)
	t.IsContinued = append(t.IsContinued, block.IsContinued)
}

func (t *Table) GetAdjustedMinTID(blockIndex uint32) uint32 {
	if t.IsContinued[blockIndex] {
		return t.MinTIDs[blockIndex] - 1
	}
	return t.MinTIDs[blockIndex]
}

func (t *Table) GetChunksCount(blockIndex uint32) uint32 {
	return t.MaxTIDs[blockIndex] - t.GetAdjustedMinTID(blockIndex) + 1
}

// GetFirstBlockIndexForTID finds first block index in file for TID
func (t *Table) GetFirstBlockIndexForTID(tid uint32) uint32 {
	if len(t.MaxTIDs) == 0 {
		logger.Panic("no blocks found for tid", zap.Uint32("tid", tid))
	}

	n := len(t.MaxTIDs)
	// The binary search predicate function must be monotonic.
	// That's why we compare with ">=" and not just with "==" (see doc for sort.Search())
	index := sort.Search(n, func(i int) bool { return t.MaxTIDs[i] >= tid })

	if index == n {
		logger.Panic("can't find block for tid",
			zap.Uint32("tid", tid),
			zap.Uint32("last_tid", t.MaxTIDs[n-1]))
	}

	return uint32(index)
}

// GetLastBlockIndexForTID finds last block index in file for TID
func (t *Table) GetLastBlockIndexForTID(tid uint32) uint32 {
	if len(t.MaxTIDs) == 0 {
		logger.Panic("no blocks found for tid", zap.Uint32("tid", tid))
	}
	n := len(t.MinTIDs)

	index := sort.Search(n, func(i int) bool { return t.GetAdjustedMinTID(uint32(i)) > tid }) - 1
	if tid > t.MaxTIDs[index] { // case of last block: index == n - 1
		logger.Panic("can't find block for tid",
			zap.Uint32("tid", tid),
			zap.Uint32("last_tid", t.MaxTIDs[n-1]))
	}

	return uint32(index)
}

func (t *Table) HasTIDInPrevBlock(blockIndex, tid uint32) bool {
	if blockIndex == 0 { // it is no prev block
		return false
	}
	if t.MaxTIDs[blockIndex-1] == tid {
		return true
	}
	return false
}

func (t *Table) HasTIDInNextBlock(blockIndex, tid uint32) bool {
	if len(t.MinTIDs)-1 == int(blockIndex) { // it is no next block
		return false
	}
	if t.GetAdjustedMinTID(blockIndex+1) == tid {
		return true
	}
	return false
}

func (t *Table) GetChunkIndex(blockIndex, tid uint32) int {
	return int(tid - t.GetAdjustedMinTID(blockIndex))
}
