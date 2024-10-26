package frac

import (
	"encoding/binary"
	"time"

	"go.uber.org/atomic"
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

type UnpackCache struct {
	LastBlock  int64
	readVals   []uint64 // actual slice for reading
	valsBuffer []uint64 // reusable buffer for writing to
	StartLID   uint64
	uses       atomic.Int64 // used to ensure there is no concurrent use of unpack cache
}

const (
	defaultValsCapacity = consts.IDsPerBlock
)

func NewUnpackCache() *UnpackCache {
	return &UnpackCache{
		LastBlock:  -1,
		StartLID:   0,
		valsBuffer: make([]uint64, 0, defaultValsCapacity),
	}
}

func (c *UnpackCache) Start() {
	if c.uses.Inc() > 1 {
		c.uses.Dec()
		logger.Panic("concurrent use of unpack cache")
	}

	c.LastBlock = -1
	c.readVals = nil
	c.StartLID = 0
}

func (c *UnpackCache) Finish() {
	if c.uses.Dec() < 0 {
		logger.Panic("too many finishes of unpack cache")
	}

	c.readVals = nil
}

func (c *UnpackCache) GetValByLID(lid uint64) uint64 {
	return c.readVals[lid-c.StartLID]
}

func (c *UnpackCache) unpackMIDs(index int64, data []byte) {
	c.LastBlock = index
	c.StartLID = uint64(index) * consts.IDsPerBlock
	c.valsBuffer = unpackRawIDsVarint(data, c.valsBuffer)
	c.readVals = c.valsBuffer
}

func (c *UnpackCache) unpackRIDs(index int64, data []byte, fracVersion BinaryDataVersion) {
	c.LastBlock = index
	c.StartLID = uint64(index) * consts.IDsPerBlock

	if fracVersion < BinaryDataV1 {
		c.valsBuffer = unpackRawIDsVarint(data, c.valsBuffer)
	} else {
		c.valsBuffer = unpackRawIDsNoVarint(data, c.valsBuffer)
	}

	c.readVals = c.valsBuffer
}

func unpackRawIDsVarint(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	id := uint64(0)
	for len(src) != 0 {
		delta, n := binary.Varint(src)
		if n <= 0 {
			panic("varint decoded with error")
		}
		src = src[n:]
		id += uint64(delta)
		dst = append(dst, id)
	}
	return dst
}

func unpackRawIDsNoVarint(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint64(src))
		src = src[8:]
	}
	return dst
}

func NewSealedIDs(reader *disk.Reader, blocksReader *disk.BlocksReader, cache *SealedIndexCache) *SealedIDs {
	return &SealedIDs{
		Reader:       reader,
		BlocksReader: blocksReader,
		cache:        cache,
	}
}

func (si *SealedIDs) GetMIDsBlock(searchSB *SearchCell, lid seq.LID, unpackCache *UnpackCache) {
	if unpackCache.uses.Load() == 0 {
		logger.Panic("unpack cache isn't started for mid")
	}

	index := si.getIDBlockIndexByLID(lid)
	if index == unpackCache.LastBlock { // fast path, already unpacked
		return
	}

	data := si.cache.MIDs.Get(uint32(index+1), func() ([]byte, int) {
		block := si.loadMIDBlock(searchSB, uint32(index))
		return block, len(block)
	})

	if len(data) == 0 {
		logger.Panic("empty mids block returned from cache",
			zap.Uint32("lid", uint32(lid)),
			zap.Int64("index", index),
		)
	}

	unpackCache.unpackMIDs(index, data)
}

func (si *SealedIDs) GetRIDsBlock(searchSB *SearchCell, lid seq.LID, unpackCache *UnpackCache, fracVersion BinaryDataVersion) {
	if unpackCache.uses.Load() == 0 {
		logger.Panic("unpack cache isn't started for rid")
	}

	index := si.getIDBlockIndexByLID(lid)
	if index == unpackCache.LastBlock { // fast path, already unpacked
		return
	}

	data := si.cache.RIDs.Get(uint32(index)+1, func() ([]byte, int) {
		block := si.loadRIDBlock(searchSB, uint32(index))
		return block, len(block)
	})

	if len(data) == 0 {
		logger.Panic("empty rids block returned from cache",
			zap.Uint32("lid", uint32(lid)),
			zap.Int64("index", index),
		)
	}

	unpackCache.unpackRIDs(index, data, fracVersion)
}

func (si *SealedIDs) GetParamsBlock(index uint32) []uint64 {
	params := si.cache.Params.Get(index+1, func() ([]uint64, int) {
		block := si.loadParamsBlock(index)
		return block, len(block) * 8
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
	readTask := si.Reader.ReadIndexBlock(si.BlocksReader, si.midBlockIndex(index), nil)
	searchSB.AddReadIDTimeNS(time.Since(t))

	if util.IsRecoveredPanicError(readTask.Err) {
		logger.Panic("todo: handle read err", zap.Error(readTask.Err))
	}

	if len(readTask.Buf) == 0 {
		logger.Panic("wrong mid block",
			zap.String("file", si.BlocksReader.GetFileName()),
			zap.Uint32("index", index),
			zap.Uint32("disk_index", si.midBlockIndex(index)),
			zap.Uint32("blocks_total", si.IDBlocksTotal),
			zap.Error(readTask.Err),
			zap.Any("min_block_ids", si.MinBlockIDs),
		)
	}

	return readTask.Buf
}

func (si *SealedIDs) loadRIDBlock(searchCell *SearchCell, index uint32) []byte {
	t := time.Now()
	readTask := si.Reader.ReadIndexBlock(si.BlocksReader, si.ridBlockIndex(index), nil)
	searchCell.AddReadIDTimeNS(time.Since(t))

	if util.IsRecoveredPanicError(readTask.Err) {
		logger.Panic("todo: handle read err", zap.Error(readTask.Err))
	}

	return readTask.Buf
}

func (si *SealedIDs) loadParamsBlock(index uint32) []uint64 {
	readTask := si.Reader.ReadIndexBlock(si.BlocksReader, si.paramsBlockIndex(index), nil)
	if util.IsRecoveredPanicError(readTask.Err) {
		logger.Panic("todo: handle read err", zap.Error(readTask.Err))
	}
	return unpackRawIDsVarint(readTask.Buf, make([]uint64, 0, consts.IDsPerBlock))
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
