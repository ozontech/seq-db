package frac

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SealedIDs struct {
	Reader       *disk.Reader
	BlocksReader *disk.BlocksReader

	IDBlocksTotal       uint32
	IDsTotal            uint32
	DiskStartBlockIndex uint32

	MinBlockIDs []seq.ID // from max to min

	cache *SealedIndexCache
}

func NewSealedIDs(reader *disk.Reader, blocksReader *disk.BlocksReader, cache *SealedIndexCache) *SealedIDs {
	return &SealedIDs{
		Reader:       reader,
		BlocksReader: blocksReader,
		cache:        cache,
	}
}

func (si *SealedIDs) GetMIDsBlock(searchSB *SearchCell, lid seq.LID, dst *UnpackCache) {
	index := si.getIDBlockIndexByLID(lid)
	if index == dst.lastBlock { // fast path, already unpacked
		return
	}

	data := si.cache.MIDs.Get(uint32(index+1), func() ([]byte, int) {
		block := si.loadMIDBlock(searchSB, uint32(index))
		return block, cap(block)
	})

	if len(data) == 0 {
		logger.Panic("empty mids block returned from cache",
			zap.Uint32("lid", uint32(lid)),
			zap.Int64("index", index),
		)
	}

	dst.unpackMIDs(index, data)
}

func (si *SealedIDs) GetRIDsBlock(searchSB *SearchCell, lid seq.LID, dst *UnpackCache, fracVersion BinaryDataVersion) {
	index := si.getIDBlockIndexByLID(lid)
	if index == dst.lastBlock { // fast path, already unpacked
		return
	}

	data := si.cache.RIDs.Get(uint32(index)+1, func() ([]byte, int) {
		block := si.loadRIDBlock(searchSB, uint32(index))
		return block, cap(block)
	})

	if len(data) == 0 {
		logger.Panic("empty rids block returned from cache",
			zap.Uint32("lid", uint32(lid)),
			zap.Int64("index", index),
		)
	}

	dst.unpackRIDs(index, data, fracVersion)
}

func (si *SealedIDs) GetParamsBlock(index uint32) []uint64 {
	params := si.cache.Params.Get(index+1, func() ([]uint64, int) {
		block := si.loadParamsBlock(index)
		return block, cap(block) * 8
	})

	if len(params) == 0 {
		logger.Panic("empty idps block returned from cache", zap.Uint32("index", index))
	}

	return params
}

// blocks are stored as triplets on disk, (MID + RID + Pos), check docs/format-index-file.go
func (si *SealedIDs) midBlockIndex(index uint32) uint32 {
	return si.DiskStartBlockIndex + index*3
}

func (si *SealedIDs) ridBlockIndex(index uint32) uint32 {
	return si.DiskStartBlockIndex + index*3 + 1
}

func (si *SealedIDs) paramsBlockIndex(index uint32) uint32 {
	return si.DiskStartBlockIndex + index*3 + 2
}

func (si *SealedIDs) loadMIDBlock(searchSB *SearchCell, index uint32) []byte {
	t := time.Now()
	data, _, err := si.Reader.ReadIndexBlock(si.BlocksReader, si.midBlockIndex(index), nil)
	searchSB.AddReadIDTimeNS(time.Since(t))

	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	if len(data) == 0 {
		logger.Panic("wrong mid block",
			zap.String("file", si.BlocksReader.GetFileName()),
			zap.Uint32("index", index),
			zap.Uint32("disk_index", si.midBlockIndex(index)),
			zap.Uint32("blocks_total", si.IDBlocksTotal),
			zap.Error(err),
			zap.Any("min_block_ids", si.MinBlockIDs),
		)
	}

	return data
}

func (si *SealedIDs) loadRIDBlock(searchCell *SearchCell, index uint32) []byte {
	t := time.Now()
	data, _, err := si.Reader.ReadIndexBlock(si.BlocksReader, si.ridBlockIndex(index), nil)
	searchCell.AddReadIDTimeNS(time.Since(t))

	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	return data
}

func (si *SealedIDs) loadParamsBlock(index uint32) []uint64 {
	data, _, err := si.Reader.ReadIndexBlock(si.BlocksReader, si.paramsBlockIndex(index), nil)

	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}
	return unpackRawIDsVarint(data, make([]uint64, 0, consts.IDsPerBlock))
}

func (si *SealedIDs) getIDBlockIndexByLID(lid seq.LID) int64 {
	return int64(lid) / consts.IDsPerBlock
}

func (si *SealedIDs) GetDocPosByLID(lid seq.LID) DocPos {
	index := si.getIDBlockIndexByLID(lid)
	positions := si.GetParamsBlock(uint32(index))
	startLID := index * consts.IDsPerBlock
	i := lid - seq.LID(startLID)
	return DocPos(positions[i])
}

// GetDocPosByLIDs returns a slice of DocPos for the corresponding LIDs.
// Passing sorted LIDs (asc or desc) will improve the performance of this method.
// For LID with zero value will return DocPos with `DocPosNotFound` value
func (si *SealedIDs) GetDocPosByLIDs(lids []seq.LID) []DocPos {
	var (
		prevIndex int64
		positions []uint64
		startLID  seq.LID
	)

	res := make([]DocPos, len(lids))
	for i, lid := range lids {
		if lid == 0 {
			res[i] = DocPosNotFound
			continue
		}

		index := si.getIDBlockIndexByLID(lid)
		if positions == nil || prevIndex != index {
			positions = si.GetParamsBlock(uint32(index))
			startLID = seq.LID(index * consts.IDsPerBlock)
		}

		res[i] = DocPos(positions[lid-startLID])
	}

	return res
}
