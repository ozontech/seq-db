package active

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
)

// indexEntry is an internal structure that describes a memIndex
// inside the pool and its state during merge operations.
type indexEntry struct {
	id    uint64    // unique runtime ID of the index
	index *memIndex // pointer to the actual index
	gen   int       // generation, used for merge management
}

// memIndexPool manages the lifecycle of in-memory indexes:
//   - keeps indexes ready for use
//   - tracks indexes currently participating in merge
//   - provides consistent snapshots for readers
type memIndexPool struct {
	mu     sync.RWMutex // protects all fields below
	info   *frac.Info   // aggregated information for all indexes
	hashes map[uint64]struct{}

	ready   map[uint64]indexEntry // indexes ready to be merged
	merging map[uint64]indexEntry // indexes currently being merged

	// readable is a flat list of indexes available for reading.
	// It contains both ready and merging indexes.
	readable []*memIndex

	nextID atomic.Uint64 // atomic counter for generating index IDs
}

// NewIndexPool creates a new index pool
func NewIndexPool(info *frac.Info) *memIndexPool {
	return &memIndexPool{
		info:    info,
		hashes:  make(map[uint64]struct{}, 1000),
		ready:   make(map[uint64]indexEntry),
		merging: make(map[uint64]indexEntry),
	}
}

// indexSnapshot represents a consistent snapshot of the pool state.
// It is used to safely read indexes without holding the pool lock.
type indexSnapshot struct {
	info    *frac.Info  // copy of aggregated info
	indexes []*memIndex // indexes available for reading
}

// Snapshot returns a snapshot and a release function.
// While the snapshot is alive, indexes are protected from being released via wg.
func (p *memIndexPool) Snapshot() (*indexSnapshot, func()) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Copy info so the snapshot is immutable
	info := *p.info

	iss := indexSnapshot{
		info:    &info,
		indexes: make([]*memIndex, len(p.readable)),
	}

	// Increment usage counter for each index
	for i, idx := range p.readable {
		iss.indexes[i] = idx
		idx.wg.Add(1)
	}

	return &iss, func() {
		// release function decrements wg counters
		for _, idx := range iss.indexes {
			idx.wg.Done()
		}
	}
}

// Info returns a copy of the aggregated pool information
func (p *memIndexPool) Info() *frac.Info {
	p.mu.RLock()
	defer p.mu.RUnlock()

	info := *p.info // copy
	return &info
}

// Add adds a new memIndex to the pool and updates aggregated statistics
func (p *memIndexPool) Add(idx *memIndex, docsLen, metaLen uint64) {
	maxMID := idx.ids[0].MID
	minMID := idx.ids[len(idx.ids)-1].MID

	entry := p.newEntry(idx, 0)

	p.mu.Lock()
	defer p.mu.Unlock()

	if idx.hash > 0 {
		if _, ok := p.hashes[idx.hash]; ok {
			logger.Warn("a duplicate index (bulk) has been detected")
			return
		}
		p.hashes[idx.hash] = struct{}{}
	}

	if p.info.From > minMID {
		p.info.From = minMID
	}
	if p.info.To < maxMID {
		p.info.To = maxMID
	}

	p.info.DocsRaw += idx.docsSize
	p.info.DocsTotal += idx.docsCount

	p.info.DocsOnDisk += docsLen
	p.info.MetaOnDisk += metaLen

	p.ready[entry.id] = entry
	p.readable = append(p.readable, idx)
}

// ReadyToMerge returns indexes that can be taken for merge (returns a copy without modifying the pool state)
func (p *memIndexPool) ReadyToMerge() []indexEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	entries := make([]indexEntry, 0, len(p.ready))
	for _, entry := range p.ready {
		entries = append(entries, entry)
	}
	return entries
}

// takeForMerge moves indexes from the "ready" state to the "merging" state
func (p *memIndexPool) takeForMerge(entries []indexEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, entry := range entries {
		delete(p.ready, entry.id)
		p.merging[entry.id] = entry
	}
}

// replace replaces several old indexes with a single merged index
func (p *memIndexPool) replace(old []indexEntry, merged *memIndex) {
	newEntry := p.newEntry(merged, avgGeneration(old)+1)

	defer func() {
		for _, entry := range old {
			entry.index.Release()
		}
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	var docsCountToRemove uint32
	for _, entry := range old {
		docsCountToRemove += entry.index.docsCount
		delete(p.merging, entry.id)
	}
	p.ready[newEntry.id] = newEntry

	// update info: the number of documents to be deleted may be greater
	// than the number to be added due to deduplication
	if docsCountToRemove > p.info.DocsTotal {
		panic("inconsistent state of index pool")
	}
	p.info.DocsTotal -= uint32(docsCountToRemove)
	p.info.DocsTotal += newEntry.index.docsCount

	p.rebuildReadable()
}

// avgGeneration calculates the average generation of indexes
func avgGeneration(entries []indexEntry) int {
	gen := 0
	for _, entry := range entries {
		gen += entry.gen
	}
	return gen / len(entries)
}

// rebuildReadable rebuilds the list of indexes available for reading (ready + merging)
func (p *memIndexPool) rebuildReadable() {
	p.readable = p.readable[:0]
	p.readable = slices.Grow(p.readable, len(p.ready)+len(p.merging))

	for _, entry := range p.ready {
		p.readable = append(p.readable, entry.index)
	}
	for _, entry := range p.merging {
		p.readable = append(p.readable, entry.index)
	}
}

// Release fully releases the pool and all contained indexes
func (p *memIndexPool) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, idx := range p.readable {
		idx.Release()
	}

	p.readable = nil
	p.ready = nil
	p.merging = nil
}

// newEntry creates a new indexEntry with a unique ID
func (p *memIndexPool) newEntry(index *memIndex, gen int) indexEntry {
	return indexEntry{
		id:    p.nextID.Add(1),
		gen:   gen,
		index: index,
	}
}
