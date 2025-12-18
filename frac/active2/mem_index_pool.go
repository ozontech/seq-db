package active2

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/alecthomas/units"
)

// memIndexExt contains index metadata for merge management
type memIndexExt struct {
	id    uint64    // unique runtime ID
	index *memIndex // actual index
	tier  int       // size tier of the index
}

type memIndexPool struct {
	mu           sync.RWMutex
	indexes      []*memIndex
	readyToMerge map[uint64]memIndexExt
	underMerging map[uint64]memIndexExt

	tiers   sizeTiers     // index size tier classifier
	counter atomic.Uint64 // atomic counter for generating index IDs
}

func newIndexPool() *memIndexPool {
	return &memIndexPool{
		readyToMerge: make(map[uint64]memIndexExt),
		underMerging: make(map[uint64]memIndexExt),

		tiers: newSizeTiers(firstTierMaxSizeKb, maxTierCount, tierSizeDeltaPercent),
	}
}

func (p *memIndexPool) Indexes() []*memIndex {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.indexes
}

func (p *memIndexPool) Add(index *memIndex) {
	metaIndex := p.wrapIndex(index)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.readyToMerge[metaIndex.id] = metaIndex
	p.indexes = append(p.indexes, index)
}

func (p *memIndexPool) ReadyToMerge() []memIndexExt {
	p.mu.RLock()
	defer p.mu.RUnlock()

	items := make([]memIndexExt, 0, len(p.readyToMerge))
	for _, item := range p.readyToMerge {
		items = append(items, item)
	}
	return items
}

// markAsMerging moves indexes from "ready" to "merging" state
func (p *memIndexPool) markAsMerging(items []memIndexExt) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, item := range items {
		delete(p.readyToMerge, item.id)
		p.underMerging[item.id] = item
	}
}

func (p *memIndexPool) replace(oldIndexes []memIndexExt, newIndex *memIndex) {
	mergedMeta := p.wrapIndex(newIndex)

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, metaIndex := range oldIndexes {
		delete(p.underMerging, metaIndex.id)
	}
	p.readyToMerge[mergedMeta.id] = mergedMeta

	// Rebuild the index list for reading
	p.indexes = p.indexes[:0]
	p.indexes = slices.Grow(p.indexes, len(p.readyToMerge)+len(p.underMerging))

	for _, metaIndex := range p.readyToMerge {
		p.indexes = append(p.indexes, metaIndex.index) // add all ready indexes
	}
	for _, metaIndex := range p.underMerging {
		p.indexes = append(p.indexes, metaIndex.index) // add indexes currently being merged
	}

	for _, metaIndex := range oldIndexes {
		metaIndex.index.Release()
	}
}

func (p *memIndexPool) wrapIndex(index *memIndex) memIndexExt {
	return memIndexExt{
		id:    p.counter.Add(1),                                  // atomically increment counter
		tier:  p.tiers.Calc(index.docsCount / uint32(units.KiB)), // determine size tier
		index: index,
	}
}
