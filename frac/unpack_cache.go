package frac

import (
	"encoding/binary"
	"sync"

	"github.com/ozontech/seq-db/consts"
)

type UnpackCache struct {
	lastBlock int64
	startLID  uint64
	values    []uint64
}

var unpackCachePool = sync.Pool{}

const defaultValsCapacity = consts.IDsPerBlock

func NewUnpackCache() *UnpackCache {
	o := unpackCachePool.Get()
	if c, ok := o.(*UnpackCache); ok {
		return c.reset()
	}

	return &UnpackCache{
		lastBlock: -1,
		startLID:  0,
		values:    make([]uint64, 0, defaultValsCapacity),
	}
}

func (c *UnpackCache) reset() *UnpackCache {
	c.lastBlock = -1
	c.startLID = 0
	c.values = c.values[:0]
	return c
}

func (c *UnpackCache) Release() {
	unpackCachePool.Put(c)
}

func (c *UnpackCache) GetValByLID(lid uint64) uint64 {
	return c.values[lid-c.startLID]
}

func (c *UnpackCache) unpackMIDs(index int64, data []byte) {
	c.lastBlock = index
	c.startLID = uint64(index) * consts.IDsPerBlock
	c.values = unpackRawIDsVarint(data, c.values)
}

func (c *UnpackCache) unpackRIDs(index int64, data []byte, fracVersion BinaryDataVersion) {
	c.lastBlock = index
	c.startLID = uint64(index) * consts.IDsPerBlock

	if fracVersion < BinaryDataV1 {
		c.values = unpackRawIDsVarint(data, c.values)
		return
	}

	c.values = unpackRawIDsNoVarint(data, c.values)
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
