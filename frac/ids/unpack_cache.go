package ids

import (
	"sync"

	"github.com/ozontech/seq-db/conf"
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

func (c *UnpackCache) unpackRIDs(index int64, data []byte, fracVersion conf.BinaryDataVersion) {
	c.lastBlock = index
	c.startLID = uint64(index) * consts.IDsPerBlock

	if fracVersion < conf.BinaryDataV1 {
		c.values = unpackRawIDsVarint(data, c.values)
		return
	}

	c.values = unpackRawIDsNoVarint(data, c.values)
}
