package seqids

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/conf"
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
	reader      *disk.IndexReader
	Table       IDsTable
	cacheMIDs   *cache.Cache[[]byte]
	cacheRIDs   *cache.Cache[[]byte]
	cacheParams *cache.Cache[[]uint64]
}

func NewIDsLoader(
	indexReader *disk.IndexReader,
	cacheMIDs *cache.Cache[[]byte],
	cacheRIDs *cache.Cache[[]byte],
	cacheParams *cache.Cache[[]uint64],
	table IDsTable,
) *IDsLoader {
	return &IDsLoader{
		reader:      indexReader,
		Table:       table,
		cacheMIDs:   cacheMIDs,
		cacheRIDs:   cacheRIDs,
		cacheParams: cacheParams,
	}
}

func (il *IDsLoader) GetMIDsBlock(lid seq.LID, dst *UnpackCache) {
	index := il.GetIDBlockIndexByLID(lid)
	if index == dst.lastBlock { // fast path, already unpacked
		return
	}

	data := il.cacheMIDs.Get(uint32(index+1), func() ([]byte, int) {
		block := il.loadMIDBlock(uint32(index))
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

func (il *IDsLoader) GetRIDsBlock(lid seq.LID, dst *UnpackCache, fracVersion conf.BinaryDataVersion) {
	index := il.GetIDBlockIndexByLID(lid)
	if index == dst.lastBlock { // fast path, already unpacked
		return
	}

	data := il.cacheRIDs.Get(uint32(index)+1, func() ([]byte, int) {
		block := il.loadRIDBlock(uint32(index))
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

func (il *IDsLoader) GetParamsBlock(index uint32) []uint64 {
	params := il.cacheParams.Get(index+1, func() ([]uint64, int) {
		block := il.loadParamsBlock(index)
		return block, cap(block) * 8
	})

	if len(params) == 0 {
		logger.Panic("empty idps block returned from cache", zap.Uint32("index", index))
	}

	return params
}

// blocks are stored as triplets on disk, (MID + RID + Pos), check docs/format-index-file.go
func (il *IDsLoader) midBlockIndex(index uint32) uint32 {
	return il.Table.DiskStartBlockIndex + index*3
}

func (il *IDsLoader) ridBlockIndex(index uint32) uint32 {
	return il.Table.DiskStartBlockIndex + index*3 + 1
}

func (il *IDsLoader) paramsBlockIndex(index uint32) uint32 {
	return il.Table.DiskStartBlockIndex + index*3 + 2
}

func (il *IDsLoader) loadMIDBlock(index uint32) []byte {
	data, _, err := il.reader.ReadIndexBlock(il.midBlockIndex(index), nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	if len(data) == 0 {
		logger.Panic("wrong mid block",
			zap.Uint32("index", index),
			zap.Uint32("disk_index", il.midBlockIndex(index)),
			zap.Uint32("blocks_total", il.Table.IDBlocksTotal),
			zap.Any("min_block_ids", il.Table.MinBlockIDs),
			zap.Error(err),
		)
	}

	return data
}

func (il *IDsLoader) loadRIDBlock(index uint32) []byte {
	data, _, err := il.reader.ReadIndexBlock(il.ridBlockIndex(index), nil)

	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	return data
}

func (il *IDsLoader) loadParamsBlock(index uint32) []uint64 {
	data, _, err := il.reader.ReadIndexBlock(il.paramsBlockIndex(index), nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}
	return unpackRawIDsVarint(data, make([]uint64, 0, consts.IDsPerBlock))
}

func (il *IDsLoader) GetIDBlockIndexByLID(lid seq.LID) int64 {
	return int64(lid) / consts.IDsPerBlock
}
