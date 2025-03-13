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

type IDsTable struct {
	MinBlockIDs         []seq.ID // from max to min
	IDBlocksTotal       uint32
	IDsTotal            uint32
	DiskStartBlockIndex uint32
}

type IDsLoader struct {
	reader *disk.IndexReader
	table  IDsTable
	cache  *IndexCache
}

func NewIDsLoader(indexReader *disk.IndexReader, indexCache *IndexCache, table IDsTable) *IDsLoader {
	return &IDsLoader{
		reader: indexReader,
		cache:  indexCache,
		table:  table,
	}
}

func (si *IDsLoader) GetMIDsBlock(searchSB *SearchCell, lid seq.LID, dst *UnpackCache) {
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

func (si *IDsLoader) GetRIDsBlock(searchSB *SearchCell, lid seq.LID, dst *UnpackCache, fracVersion BinaryDataVersion) {
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

func (si *IDsLoader) GetParamsBlock(index uint32) []uint64 {
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
func (si *IDsLoader) midBlockIndex(index uint32) uint32 {
	return si.table.DiskStartBlockIndex + index*3
}

func (si *IDsLoader) ridBlockIndex(index uint32) uint32 {
	return si.table.DiskStartBlockIndex + index*3 + 1
}

func (si *IDsLoader) paramsBlockIndex(index uint32) uint32 {
	return si.table.DiskStartBlockIndex + index*3 + 2
}

func (si *IDsLoader) loadMIDBlock(searchSB *SearchCell, index uint32) []byte {
	t := time.Now()
	data, _, err := si.reader.ReadIndexBlock(si.midBlockIndex(index), nil)
	searchSB.AddReadIDTimeNS(time.Since(t))

	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	if len(data) == 0 {
		logger.Panic("wrong mid block",
			zap.String("file", si.reader.File.Name()),
			zap.Uint32("index", index),
			zap.Uint32("disk_index", si.midBlockIndex(index)),
			zap.Uint32("blocks_total", si.table.IDBlocksTotal),
			zap.Any("min_block_ids", si.table.MinBlockIDs),
			zap.Error(err),
		)
	}

	return data
}

func (si *IDsLoader) loadRIDBlock(searchCell *SearchCell, index uint32) []byte {
	t := time.Now()
	data, _, err := si.reader.ReadIndexBlock(si.ridBlockIndex(index), nil)
	searchCell.AddReadIDTimeNS(time.Since(t))

	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	return data
}

func (si *IDsLoader) loadParamsBlock(index uint32) []uint64 {
	data, _, err := si.reader.ReadIndexBlock(si.paramsBlockIndex(index), nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}
	return unpackRawIDsVarint(data, make([]uint64, 0, consts.IDsPerBlock))
}

func (si *IDsLoader) getIDBlockIndexByLID(lid seq.LID) int64 {
	return int64(lid) / consts.IDsPerBlock
}

func (si *IDsLoader) GetDocPosByLID(lid seq.LID) DocPos {
	index := si.getIDBlockIndexByLID(lid)
	positions := si.GetParamsBlock(uint32(index))
	startLID := index * consts.IDsPerBlock
	i := lid - seq.LID(startLID)
	return DocPos(positions[i])
}
