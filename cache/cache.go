package cache

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

const (
	clearBias           = 0.25
	excessiveSizeFactor = 8
)

type generation struct {
	entryCount   int
	creationTime int64
	stale        bool
}

func newGeneration() *generation {
	return &generation{
		creationTime: time.Now().UnixNano(),
	}
}

// check docs/cache.md first
type entry[V any] struct {
	// value is written under Cache.mu lock
	// can be read without lock if wg is nil or waited on
	value V
	// wg is written under Cache.mu lock
	// can be read without lock if was waited on
	// (after previous read under lock)
	// wg allows to wait for value to be ready
	// and indicates entry current state
	// if not nil, wait on it
	// if after waiting it's still here,
	// value wasn't initialized and entry is abandoned, retry
	// if wg is nil, everything is good, value can be used
	wg *sync.WaitGroup
	// gen is written and read only under Cache.mu lock
	gen *generation
	// size is written under Cache.mu lock
	// can be read without lock if wg is nil or waited on
	size int
}

func (e *entry[V]) updateGeneration(ng *generation) {
	if og := e.gen; og != nil {
		og.entryCount--
		ng.entryCount++
		e.gen = ng
	}
}

type Cache[V any] struct {
	payload map[uint32]*entry[V]
	// max len payload map had
	maxPayloadSize    int
	currentGeneration *generation
	generations       []*generation
	// newEntries are the entries that don't have value yet, but are present in payload
	newEntries int
	// creation time of the oldest not-yet-collected generation (possibly stale though)
	oldestCreationTime int64
	totalSize          *atomic.Uint64
	// covers all Cache operations
	mu      sync.Mutex
	metrics *Metrics
}

func NewCache[V any](
	totalSize *atomic.Uint64,
	metrics *Metrics,
) *Cache[V] {
	firstGeneration := newGeneration()
	l := &Cache[V]{
		payload:            make(map[uint32]*entry[V]),
		currentGeneration:  firstGeneration,
		generations:        []*generation{firstGeneration},
		oldestCreationTime: firstGeneration.creationTime,
		totalSize:          totalSize,
		metrics:            metrics,
	}

	return l
}

// Reset is used in tests only
func (c *Cache[V]) Reset() {
	newPayload := make(map[uint32]*entry[V])
	firstGeneration := newGeneration()
	generations := []*generation{firstGeneration}
	c.mu.Lock()
	c.payload = newPayload
	c.maxPayloadSize = 0
	c.currentGeneration = firstGeneration
	c.generations = generations
	c.newEntries = 0
	c.oldestCreationTime = firstGeneration.creationTime
	c.mu.Unlock()
}

func (c *Cache[V]) setGeneration(newGeneration *generation) {
	c.currentGeneration = newGeneration
	c.generations = append(c.generations, newGeneration)
}

func (c *Cache[V]) StartNewGeneration() {
	gen := newGeneration()
	c.mu.Lock()
	c.setGeneration(gen)
	c.mu.Unlock()
}

func (c *Cache[V]) RetireOldGenerations(desiredGenerations int) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	// mark stale
	for len(c.generations) > desiredGenerations {
		c.generations[0].stale = true
		c.generations = c.generations[1:]
	}

	if len(c.generations) == 0 {
		// have at least one current generation
		c.setGeneration(newGeneration())
	}

	// check if we can collect enough garbage
	staleEntries := len(c.payload) - c.newEntries
	for _, g := range c.generations {
		staleEntries -= g.entryCount
	}
	if float64(staleEntries) < float64(len(c.payload))*clearBias {
		return 0
	}

	// collect old data
	totalFreed := 0
	if len(c.payload) > c.maxPayloadSize {
		c.maxPayloadSize = len(c.payload)
	}
	for k, e := range c.payload {
		if e.gen == nil || !e.gen.stale {
			continue
		}
		delete(c.payload, k)
		totalFreed += e.size
	}
	if c.totalSize != nil {
		c.totalSize.Sub(uint64(totalFreed))
	}

	// recreate payload map if len is too small
	// probably fraction is old and useless
	if len(c.payload)*excessiveSizeFactor < c.maxPayloadSize {
		newPayload := make(map[uint32]*entry[V], len(c.payload)*2)
		for k, v := range c.payload {
			newPayload[k] = v
		}
		c.payload = newPayload
		c.maxPayloadSize = len(c.payload)
	}

	c.oldestCreationTime = c.generations[0].creationTime

	if c.metrics != nil {
		c.metrics.SizeReleasedTotal.Add(float64(totalFreed))
	}
	return totalFreed
}

// getOrCreate attempts to get value from cache
// in case of failure it creates new entry, puts it into cache and returns
func (c *Cache[V]) getOrCreate(key uint32) (*entry[V], *sync.WaitGroup, bool) {
	c.reportTouch()

	if !c.mu.TryLock() {
		// we only need this for metrics
		c.reportLockWait()
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
			c.reportWait()
			wg.Wait()
		}
		// when wg is done or nil, wg, size and value no longer change
		// there's no need for locks
		if e.wg == nil {
			// entry is valid, value is in cache
			c.reportHit()
			c.reportHitsSize(e.size)
			return e, nil, true
		}
		// someone messed it up, we need to reattempt
		// this should happen rarely and only due to fn panics
		c.reportReattempt()
		c.mu.Lock()
		e, ok = c.payload[key]
	}
	// we are to put the value into cache ourselves
	e = &entry[V]{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	e.wg = wg
	c.payload[key] = e
	c.newEntries++
	c.mu.Unlock()

	return e, wg, false
}

// save is called when everything went well, and we want to save the value in cache
func (c *Cache[V]) save(e *entry[V], wg *sync.WaitGroup, value V, size int) {
	c.mu.Lock()
	g := c.currentGeneration
	g.entryCount++
	e.value = value
	e.size = size
	e.gen = g
	// from now on entry is valid
	e.wg = nil
	c.newEntries--
	c.mu.Unlock()
	if c.totalSize != nil {
		c.totalSize.Add(uint64(size))
	}

	// inform all waiters that the value is ready
	wg.Done()

	c.reportMiss()
	c.reportMissSize(uint64(size))
}

// recover is called when something went wrong, and we need to recover from unsuccessful attempt
func (c *Cache[V]) recover(key uint32, wg *sync.WaitGroup) {
	c.mu.Lock()
	delete(c.payload, key)
	c.newEntries--
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
	c.reportPanic()
	panic(err)
}

func (c *Cache[V]) Get(key uint32, fn func() (V, int)) V {
	// attempt to obtain cached value
	// or create an entry for a new one
	e, wg, success := c.getOrCreate(key)
	if success {
		return e.value
	}

	defer c.handlePanic(key, wg)
	// long operation
	value, size := fn()

	// all good, just update the cache
	c.save(e, wg, value, size)

	return value
}

func (c *Cache[V]) GetWithError(key uint32, fn func() (V, int, error)) (V, error) {
	e, wg, success := c.getOrCreate(key)
	if success {
		return e.value, nil
	}

	defer c.handlePanic(key, wg)
	value, size, err := fn()

	if err != nil {
		c.recover(key, wg)
		return value, err
	}

	c.save(e, wg, value, size)

	return value, nil
}

func (c *Cache[V]) GetCreationTime() int64 {
	return c.oldestCreationTime
}

func (c *Cache[V]) reportTouch() {
	if c.metrics != nil {
		c.metrics.TouchTotal.Inc()
	}
}

func (c *Cache[V]) reportHit() {
	if c.metrics != nil {
		c.metrics.HitsTotal.Inc()
	}
}

func (c *Cache[V]) reportMiss() {
	if c.metrics != nil {
		c.metrics.MissTotal.Inc()
	}
}

func (c *Cache[V]) reportPanic() {
	if c.metrics != nil {
		c.metrics.PanicsTotal.Inc()
	}
}

func (c *Cache[V]) reportLockWait() {
	if c.metrics != nil {
		c.metrics.LockWaitsTotal.Inc()
	}
}

func (c *Cache[V]) reportWait() {
	if c.metrics != nil {
		c.metrics.WaitsTotal.Inc()
	}
}

func (c *Cache[V]) reportReattempt() {
	if c.metrics != nil {
		c.metrics.ReattemptsTotal.Inc()
	}
}

func (c *Cache[V]) reportHitsSize(size int) {
	if c.metrics != nil {
		c.metrics.HitsSizeTotal.Add(float64(size))
	}
}

func (c *Cache[V]) reportMissSize(size uint64) {
	if c.metrics != nil {
		c.metrics.MissSizeTotal.Add(float64(size))
	}
}
