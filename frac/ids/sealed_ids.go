package ids

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

type Table struct {
	MinBlockIDs         []seq.ID // from max to min
	IDBlocksTotal       uint32
	IDsTotal            uint32
	DiskStartBlockIndex uint32
}

type Loader struct {
	Table       Table
	reader      *disk.IndexReader
	cacheMIDs   *cache.Cache[[]byte]
	cacheRIDs   *cache.Cache[[]byte]
	cacheParams *cache.Cache[[]uint64]
}

func NewLoader(
	indexReader *disk.IndexReader,
	cacheMIDs *cache.Cache[[]byte],
	cacheRIDs *cache.Cache[[]byte],
	cacheParams *cache.Cache[[]uint64],
	table Table,
) *Loader {
	return &Loader{
		reader:      indexReader,
		Table:       table,
		cacheMIDs:   cacheMIDs,
		cacheRIDs:   cacheRIDs,
		cacheParams: cacheParams,
	}
}

func (l *Loader) GetMIDsBlock(lid seq.LID, dst *UnpackCache) {
	index := l.GetIDBlockIndexByLID(lid)
	if index == dst.lastBlock { // fast path, already unpacked
		return
	}

	data := l.cacheMIDs.Get(uint32(index+1), func() ([]byte, int) {
		block := l.loadMIDBlock(uint32(index))
		return block, cap(block)
	})

	if len(data) == 0 {
		logger.Panic("empty mids block",
			zap.Uint32("lid", uint32(lid)),
			zap.Int64("index", index),
		)
	}

	dst.unpackMIDs(index, data)
}

func (l *Loader) GetRIDsBlock(lid seq.LID, dst *UnpackCache, fracVersion conf.BinaryDataVersion) {
	index := l.GetIDBlockIndexByLID(lid)
	if index == dst.lastBlock { // fast path, already unpacked
		return
	}

	data := l.cacheRIDs.Get(uint32(index)+1, func() ([]byte, int) {
		block := l.loadRIDBlock(uint32(index))
		return block, cap(block)
	})

	if len(data) == 0 {
		logger.Panic("empty rids block",
			zap.Uint32("lid", uint32(lid)),
			zap.Int64("index", index),
		)
	}

	dst.unpackRIDs(index, data, fracVersion)
}

func (l *Loader) GetParamsBlock(index uint32) []uint64 {
	params := l.cacheParams.Get(index+1, func() ([]uint64, int) {
		block := l.loadParamsBlock(index)
		return block.Values, cap(block.Values) * 8
	})

	if len(params) == 0 {
		logger.Panic("empty idps block returned from cache", zap.Uint32("index", index))
	}

	return params
}

// blocks are stored as triplets on disk, (MID + RID + Pos), check docs/format-index-file.go
func (l *Loader) midBlockIndex(index uint32) uint32 {
	return l.Table.DiskStartBlockIndex + index*3
}

func (l *Loader) ridBlockIndex(index uint32) uint32 {
	return l.Table.DiskStartBlockIndex + index*3 + 1
}

func (l *Loader) paramsBlockIndex(index uint32) uint32 {
	return l.Table.DiskStartBlockIndex + index*3 + 2
}

func (l *Loader) loadMIDBlock(index uint32) []byte {
	data, _, err := l.reader.ReadIndexBlock(l.midBlockIndex(index), nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}
	return data
}

func (l *Loader) loadRIDBlock(index uint32) []byte {
	data, _, err := l.reader.ReadIndexBlock(l.ridBlockIndex(index), nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}
	return data
}

func (l *Loader) loadParamsBlock(index uint32) BlockParams {
	data, _, err := l.reader.ReadIndexBlock(l.paramsBlockIndex(index), nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	block := BlockParams{Values: make([]uint64, 0, consts.IDsPerBlock)}
	block.Unpack(data)
	return block
}

func (l *Loader) GetIDBlockIndexByLID(lid seq.LID) int64 {
	return int64(lid) / consts.IDsPerBlock
}
