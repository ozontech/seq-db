package lids

import (
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/logger"
)

type Table struct {
	StartBlockIndex uint32
	MaxTIDs         []uint32 // defines last tid for each block
	MinTIDs         []uint32 // defines first not continued tid for each block
	FirstLIDs       []uint32
	LastLIDs        []uint32

	FracVer     config.BinaryDataVersion
	IsContinued []bool // legacy field, only used in BinaryDataV0-BinaryDataV4 (inclusive)
}

func NewTable(
	fracVer config.BinaryDataVersion,
	startOfLIDsBlockIndex uint32,
	minTIDs, maxTIDs []uint32,
	firstLIDs, lastLIDs []uint32,
	isContinued []bool,
) *Table {
	return &Table{
		StartBlockIndex: startOfLIDsBlockIndex,
		MinTIDs:         minTIDs,
		MaxTIDs:         maxTIDs,
		FirstLIDs:       firstLIDs,
		LastLIDs:        lastLIDs,
		IsContinued:     isContinued,
		FracVer:         fracVer,
	}
}

func (t *Table) GetAdjustedMinTID(blockIndex uint32) uint32 {
	if t.FracVer < config.BinaryDataV6 {
		if t.IsContinued[blockIndex] {
			return t.MinTIDs[blockIndex] - 1
		}
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

// SeekBlockGeq finds next block for provided TID which contains
// lid greater or equal to provided LID starting from provided index (inclusive).
// - index: an index of block which is already suits and contains next portion of LIDs. Safe to return for old fractions.
func (t *Table) SeekBlockGeq(index, tid, nextLID uint32) uint32 {
	if t.FracVer < config.BinaryDataV6 {
		// not supported for old frac versions
		return index
	}

	res := index
	for i := int(index) + 1; i < len(t.MinTIDs); i++ {
		if t.MinTIDs[i] == tid && nextLID >= t.FirstLIDs[i] {
			res = uint32(i)
			continue
		}
		break
	}
	return res
}

// SeekBlockLeq finds next block with lowest index for provided TID which contains LIDs
// less or equal to provided LID starting from provided index (inclusive).
// - index: an index of block which is already suits and contains next portion of LIDs. Safe to return for old fractions.
func (t *Table) SeekBlockLeq(index, tid, nextLID uint32) uint32 {
	if t.FracVer < config.BinaryDataV6 {
		// not supported for old frac versions
		return index
	}

	res := index
	for i := int(index) - 1; i >= 0; i-- {
		if t.MaxTIDs[i] == tid && nextLID <= t.LastLIDs[i] {
			res = uint32(i)
			continue
		}
		break
	}
	return res
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
