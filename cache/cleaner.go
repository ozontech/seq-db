package cache

import (
	"sync"
)

const (
	maxGenerationRatio  = 0.05
	minSizeToCleanRatio = 0.05
)

type bucket interface {
	SetGeneration(*Generation)
	Cleanup() uint64
	Released() bool

	Reset(*Generation) // tests only
}

type CleanStat struct {
	TotalSize      uint64
	GensTotal      int
	BucketsTotal   int
	GensCleaned    int
	OldestGenTime  int64
	SizeToClean    uint64
	BytesReleased  uint64
	BucketsCleaned int
}
type Cleaner struct {
	sizeLimit uint64
	metrics   *CleanerMetrics

	mu          sync.Mutex
	buckets     []bucket
	generations []*Generation
	lastGen     *Generation
	maxGenSize  uint64
}

func NewCleaner(sizeLimit uint64, metrics *CleanerMetrics) *Cleaner {
	g := NewGeneration()
	metrics.GenerationsInc()
	return &Cleaner{
		sizeLimit:   sizeLimit,
		metrics:     metrics,
		generations: []*Generation{g},
		lastGen:     g,
		maxGenSize:  uint64(maxGenerationRatio * float64(sizeLimit)),
	}
}

func (c *Cleaner) SizeLimit() uint64 {
	return c.sizeLimit
}

func (c *Cleaner) AddBucket(b bucket) {
	c.mu.Lock()
	b.SetGeneration(c.lastGen)
	c.buckets = append(c.buckets, b)
	c.mu.Unlock()

	c.metrics.BucketsInc()
}

func (c *Cleaner) getBuckets() []bucket {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.buckets
}

// Reset is used in tests only
func (c *Cleaner) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastGen = NewGeneration()
	c.generations = []*Generation{c.lastGen}

	for _, b := range c.buckets {
		b.Reset(c.lastGen)
	}
}

func (c *Cleaner) getSize() uint64 {
	totalSize := uint64(0)
	for _, g := range c.generations {
		totalSize += g.size.Load()
	}
	return totalSize
}

func (c *Cleaner) Rotate() (bool, uint64) {
	// we don't need to access c.lastGen under the mutex
	// since we update it in the same goroutine
	lastGenSize := c.lastGen.size.Load()

	if c.maxGenSize == 0 || lastGenSize < c.maxGenSize {
		return false, lastGenSize
	}

	c.rotate(NewGeneration())
	return true, lastGenSize
}

func (c *Cleaner) Cleanup(stat *CleanStat) bool {
	if c.sizeLimit == 0 {
		return false
	}

	totalSize := c.getSize()

	if totalSize <= c.sizeLimit {
		return false
	}

	minSize := uint64(float64(totalSize) * minSizeToCleanRatio)
	sizeToClean := max(minSize, totalSize-c.sizeLimit)

	buckets := c.getBuckets()
	gensCleaned := c.markStale(sizeToClean)

	for _, b := range buckets {
		free := b.Cleanup()
		if free == 0 {
			continue
		}
		stat.BucketsCleaned++
		stat.BytesReleased += free
	}

	stat.TotalSize = totalSize
	stat.BucketsTotal = len(buckets)
	stat.SizeToClean = sizeToClean
	stat.GensTotal = len(c.generations)
	stat.GensCleaned = gensCleaned
	stat.OldestGenTime = c.generations[0].creationTime

	return true
}

func (c *Cleaner) CleanEmptyGenerations() int {
	j := 0
	last := len(c.generations) - 1
	for i := 0; i < last; i++ {
		if c.generations[i].size.Load() > 0 {
			c.generations[j] = c.generations[i]
			j++
		}
	}

	c.generations[j] = c.generations[last] // last always
	j++

	emptyGens := len(c.generations) - j
	c.metrics.GenerationsSub(emptyGens)
	c.generations = c.generations[:j]

	c.metrics.OldestSet(c.generations[0].creationTime)

	return emptyGens
}

func (c *Cleaner) ReleaseBuckets() int {
	toDelete := []int{}

	// collect released
	for i, b := range c.getBuckets() {
		if b.Released() {
			toDelete = append(toDelete, i)
		}
	}

	if len(toDelete) == 0 {
		return 0
	}

	// remove released
	c.mu.Lock()
	last := len(c.buckets)
	for _, i := range toDelete {
		last--
		if i >= last {
			break
		}
		c.buckets[i] = c.buckets[last]
	}
	c.buckets = c.buckets[:last]
	c.mu.Unlock()

	released := len(toDelete)
	c.metrics.BucketsSub(released)

	return released
}

func (c *Cleaner) rotate(g *Generation) {
	c.mu.Lock()
	c.lastGen = g
	for _, b := range c.buckets {
		b.SetGeneration(c.lastGen)
	}
	c.generations = append(c.generations, c.lastGen)
	c.mu.Unlock()

	c.metrics.GenerationsInc()
}

func (c *Cleaner) markStale(sizeToClean uint64) int {
	var (
		g     *Generation
		bytes uint64
		gens  int
	)

	for bytes < sizeToClean && len(c.generations) > 1 { // check all but last
		g, c.generations = c.generations[0], c.generations[1:]
		g.stale = true
		bytes += g.size.Load()
		gens++
	}

	if bytes < sizeToClean { // need clean last generation
		c.rotate(NewGeneration())
		g, c.generations = c.generations[0], c.generations[1:]
		g.stale = true
		gens++
	}

	c.metrics.GenerationsSub(gens)
	c.metrics.OldestSet(c.generations[0].creationTime)

	return gens
}
