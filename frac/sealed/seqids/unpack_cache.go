package seqids

import (
	"sync"

	"github.com/ozontech/seq-db/consts"
)

type unpackCache struct {
	blockIndex int
	startLID   uint32
	values     []uint64
}

var cachePool = sync.Pool{}

const defaultValsCapacity = consts.IDsPerBlock

func NewCache() *unpackCache {
	o := cachePool.Get()
	if c, ok := o.(*unpackCache); ok {
		return c.reset()
	}

	return &unpackCache{
		blockIndex: -1,
		startLID:   0,
		values:     make([]uint64, 0, defaultValsCapacity),
	}
}

func (c *unpackCache) reset() *unpackCache {
	c.blockIndex = -1
	c.startLID = 0
	c.values = c.values[:0]
	return c
}

func (c *unpackCache) Release() {
	cachePool.Put(c)
}

func (c *unpackCache) GetValByLID(lid uint32) uint64 {
	return c.values[lid-c.startLID]
}
