package cache

import (
	"sync"

	"go.uber.org/atomic"
)

const (
	newGenerationSize = 0.1
	minSizeToClean    = 0.05
)

type bucket interface {
	Reset() // tests only
	StartNewGeneration()
	RetireOldGenerations(desiredGenerations int) int
	GetCreationTime() int64
}

type Cleaner struct {
	layerSizes    []*atomic.Uint64
	lastCacheSize uint64
	buckets       []bucket
	generations   int
	mu            sync.Mutex
}

type CleanStat struct {
	Bytes       uint64
	Buckets     uint64
	Generations int
}

func NewCleaner(layerSizes []*atomic.Uint64) *Cleaner {
	c := &Cleaner{
		layerSizes:  layerSizes,
		generations: 1,
	}

	return c
}

func (c *Cleaner) AddBuckets(buckets ...bucket) {
	c.mu.Lock()
	c.buckets = append(c.buckets, buckets...)
	c.mu.Unlock()
}

// Reset is used in tests only
func (c *Cleaner) Reset() {
	for _, size := range c.layerSizes {
		size.Store(0)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.buckets {
		l.Reset()
	}

	c.generations = 1
}

func (c *Cleaner) getBuckets() []bucket {
	c.mu.Lock()
	buckets := c.buckets
	c.mu.Unlock()
	return buckets
}

func (c *Cleaner) GetSize() uint64 {
	totalSize := uint64(0)
	for _, size := range c.layerSizes {
		totalSize += size.Load()
	}
	return totalSize
}

func (c *Cleaner) GetCreationTimes(newest, oldest int64) (int64, int64) {
	buckets := c.getBuckets()
	for _, b := range buckets {
		creationTime := b.GetCreationTime()
		if creationTime > newest {
			newest = creationTime
		}
		if creationTime < oldest {
			oldest = creationTime
		}
	}
	return newest, oldest
}

func (c *Cleaner) CleanUp(toSize uint64, stat *CleanStat) bool {
	buckets := c.getBuckets()
	totalSize := c.GetSize()

	lastGenerationSize := totalSize - c.lastCacheSize
	if float64(lastGenerationSize) > float64(toSize)*newGenerationSize {
		c.startNewGeneration(buckets)
		c.lastCacheSize = totalSize
	}
	if totalSize <= toSize {
		return false
	}

	minSize := uint64(float64(totalSize) * minSizeToClean)
	sizeToClean := totalSize - toSize
	if sizeToClean < minSize {
		sizeToClean = minSize
	}
	bytesCleaned := c.free(buckets, sizeToClean, stat)
	c.lastCacheSize -= bytesCleaned
	return true
}

func (c *Cleaner) startNewGeneration(buckets []bucket) {
	for _, b := range buckets {
		b.StartNewGeneration()
	}
	c.generations++
}

func (c *Cleaner) free(buckets []bucket, sizeToClean uint64, stat *CleanStat) uint64 {
	bytesCleaned := uint64(0)

	for bytesCleaned < sizeToClean {
		stat.Generations++
		c.generations--
		for _, b := range buckets {
			free := b.RetireOldGenerations(c.generations)
			if free == 0 {
				continue
			}
			stat.Buckets++
			bytesCleaned += uint64(free)
		}
		if c.generations == 0 {
			c.generations = 1
		}
	}

	stat.Bytes += bytesCleaned

	return bytesCleaned
}
