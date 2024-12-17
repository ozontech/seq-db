package cache

import (
	"sync"
	"time"
	"unsafe"

	"github.com/ozontech/seq-db/bytespool"
	"go.uber.org/atomic"
)

const (
	recreateThreshold   = 200
	excessiveSizeFactor = 10
)

type Generation struct {
	size         atomic.Uint64
	creationTime int64
	stale        bool
}

func NewGeneration() *Generation {
	return &Generation{
		creationTime: time.Now().UnixNano(),
	}
}

// check docs/cache.md first

type Cache[V any] struct {
	mu                sync.Mutex // covers all Cache operations
	maxPayloadSize    int        // max len payload map had
	payload           map[uint32]*entry[V]
	currentGeneration *Generation
	entrySize         uint64
	metrics           *Metrics
	released          bool
}

func NewCache[V any](cleaner *Cleaner, metrics *Metrics) *Cache[V] {
	keySize := unsafe.Sizeof(uint32(0))
	entrySize := unsafe.Sizeof(entry[V]{}) + unsafe.Sizeof(&entry[V]{})

	res := &Cache[V]{
		payload:   make(map[uint32]*entry[V]),
		metrics:   metrics,
		entrySize: uint64(keySize + entrySize),
	}
	if cleaner != nil {
		cleaner.AddBucket(res)
	} else {
		res.SetGeneration(NewGeneration())
	}

	return res
}

// Reset is used in tests only
func (c *Cache[V]) Reset(generation *Generation) {
	newPayload := make(map[uint32]*entry[V])

	c.mu.Lock()
	c.payload = newPayload
	c.maxPayloadSize = 0
	c.currentGeneration = generation
	c.mu.Unlock()
}

func (c *Cache[V]) SetGeneration(generation *Generation) {
	c.mu.Lock()
	c.currentGeneration = generation
	c.mu.Unlock()
}

func (c *Cache[V]) Cleanup() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	// collect old data
	var totalFreed uint64
	if len(c.payload) > c.maxPayloadSize {
		c.maxPayloadSize = len(c.payload)
	}
	for k, e := range c.payload {
		if e.gen == nil || !e.gen.stale {
			continue
		}
		delete(c.payload, k)
		totalFreed += e.size
		if e.data != nil {
			e.data.Release()
		}
	}

	c.recreatePayload()
	c.metrics.reportReleased(totalFreed)

	return totalFreed
}

// Recreates the payload map. If len is too small, fraction is probably out of date and useless
func (c *Cache[V]) recreatePayload() {
	if c.maxPayloadSize < recreateThreshold { // not large enough
		return
	}
	if len(c.payload)*excessiveSizeFactor > c.maxPayloadSize { // not small enough
		return
	}

	newPayload := make(map[uint32]*entry[V], len(c.payload)*2)
	for k, v := range c.payload {
		newPayload[k] = v
	}
	c.payload = newPayload
	c.maxPayloadSize = len(c.payload)

	c.metrics.reportMapsRecreated()
}

// getOrCreate attempts to get value from cache
// in case of failure it creates new entry, puts it into cache and returns
func (c *Cache[V]) getOrCreate(key uint32) (*entry[V], *sync.WaitGroup, bool) {
	if !c.mu.TryLock() {
		// we only need this for metrics
		c.metrics.reportLockWait()
		c.mu.Lock()
	}
	// first try to retrieve value from cache
	e, ok := c.payload[key]
	for ok {
		wg := e.wg
		e.updateGeneration(c.currentGeneration)
		c.mu.Unlock()
		if wg != nil {
			// value is being added by someone else
			// we need to wait
			c.metrics.reportWait()
			wg.Wait()
		}
		// when wg is done or nil, wg, size and value no longer change
		// there's no need for locks
		if e.wg == nil {
			// entry is valid, value is in cache
			c.metrics.reportHits(e.size)
			return e, nil, true
		}
		// someone messed it up, we need to reattempt
		// this should happen rarely and only due to fn panics
		c.metrics.reportReattempt()
		c.mu.Lock()
		e, ok = c.payload[key]
	}
	// we are to put the value into cache ourselves
	e = &entry[V]{gen: c.currentGeneration}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	e.wg = wg
	c.payload[key] = e
	c.mu.Unlock()

	return e, wg, false
}

// save is called when everything went well, and we want to save the value in cache
// refMemSize - this is the size of the memory that the entry refers to
func (c *Cache[V]) save(e *entry[V], wg *sync.WaitGroup, data bytespool.Releasable[V], refMemSize int, latency float64) {
	size := c.entrySize + uint64(refMemSize)

	c.mu.Lock()

	e.data = data
	e.size = size
	e.gen.size.Add(size)

	// from now on entry is valid
	e.wg = nil
	c.mu.Unlock()

	// inform all waiters that the value is ready
	wg.Done()

	c.metrics.reportMiss(size, latency)
}

// recover is called when something went wrong, and we need to recover from unsuccessful attempt
func (c *Cache[V]) recover(key uint32, wg *sync.WaitGroup) {
	c.mu.Lock()
	delete(c.payload, key)
	c.mu.Unlock()
	// let everyone learn we messed it up
	wg.Done()
}

// handlePanic should be called directly with defer keyword
func (c *Cache[V]) handlePanic(key uint32, wg *sync.WaitGroup) {
	err := recover()
	if err == nil {
		return
	}
	// we need to remove invalid entry from cache
	c.recover(key, wg)
	c.metrics.reportPanic()
	panic(err)
}

func (c *Cache[V]) Get(key uint32, fn func() (V, int)) V {
	// attempt to obtain cached value
	// or create an entry for a new one
	e, wg, success := c.getOrCreate(key)
	if success {
		return e.data.Value()
	}

	defer c.handlePanic(key, wg)
	// long operation
	t := time.Now()
	value, refMemSize := fn()
	latency := time.Since(t).Seconds()

	// all good, just update the cache
	c.save(e, wg, newFakeReleasable[V](value), refMemSize, latency)

	return value
}

func (c *Cache[V]) GetWithError(key uint32, fn func() (V, int, error)) (V, error) {
	e, wg, success := c.getOrCreate(key)
	if success {
		return e.data.Value(), nil
	}

	defer c.handlePanic(key, wg)
	t := time.Now()
	value, refMemSize, err := fn()
	latency := time.Since(t).Seconds()

	if err != nil {
		c.recover(key, wg)
		return value, err
	}

	c.save(e, wg, newFakeReleasable[V](value), refMemSize, latency)

	return value, nil
}

func (c *Cache[V]) GetReleasableWithError(key uint32, fn func() (bytespool.Releasable[V], int, error)) (bytespool.Releasable[V], error) {
	e, wg, success := c.getOrCreate(key)
	if success {
		return e.data.CopyForRead(), nil
	}

	defer c.handlePanic(key, wg)
	t := time.Now()

	data, refMemSize, err := fn()
	latency := time.Since(t).Seconds()

	if err != nil {
		c.recover(key, wg)
		return nil, err
	}

	c.save(e, wg, data, refMemSize, latency)

	return data.CopyForRead(), nil
}

func (c *Cache[V]) GetReleasable(key uint32, fn func() (bytespool.Releasable[V], int)) bytespool.Releasable[V] {
	// attempt to obtain cached value
	// or create an entry for a new one
	e, wg, success := c.getOrCreate(key)
	if success {
		return e.data.CopyForRead()
	}

	defer c.handlePanic(key, wg)
	// long operation
	t := time.Now()

	data, refMemSize := fn()
	latency := time.Since(t).Seconds()

	// all good, just update the cache

	c.save(e, wg, data, refMemSize, latency)

	return data.CopyForRead()
}

func (c *Cache[V]) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var totalFreed uint64
	for _, e := range c.payload {
		totalFreed += e.size
		e.gen.size.Sub(e.size)
		if e.data != nil {
			e.data.Release()
		}
	}

	c.metrics.reportReleased(totalFreed)

	c.payload = nil
	c.released = true
}

func (c *Cache[V]) Released() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.released
}
