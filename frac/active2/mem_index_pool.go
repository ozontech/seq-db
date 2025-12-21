package active2

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ozontech/seq-db/frac"
)

// memIndexExt contains index metadata for merge management
type memIndexExt struct {
	id    uint64    // unique runtime ID
	index *memIndex // actual index
	gen   int
}

type memIndexPool struct {
	mu           sync.RWMutex
	info         *frac.Info
	indexes      []*memIndex
	readyToMerge map[uint64]memIndexExt
	underMerging map[uint64]memIndexExt

	counter atomic.Uint64 // atomic counter for generating index IDs
}

func NewIndexPool(info *frac.Info) *memIndexPool {
	return &memIndexPool{
		info:         info,
		readyToMerge: make(map[uint64]memIndexExt),
		underMerging: make(map[uint64]memIndexExt),
	}
}

type indexSnapshot struct {
	info    *frac.Info
	indexes []*memIndex
}

func (p *memIndexPool) Snapshot() (*indexSnapshot, func()) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	info := *p.info // copy
	iss := indexSnapshot{
		info:    &info,
		indexes: make([]*memIndex, len(p.indexes)),
	}
	for i, idx := range p.indexes {
		iss.indexes[i] = idx
		idx.wg.Add(1)
	}

	return &iss, func() {
		for _, idx := range iss.indexes {
			idx.wg.Done()
		}
	}
}

func (p *memIndexPool) Info() *frac.Info {
	p.mu.RLock()
	defer p.mu.RUnlock()

	info := *p.info // copy
	return &info
}

func (p *memIndexPool) Add(idx *memIndex, docsLen, metaLen uint64) {
	maxMID := idx.ids[0].MID
	minMID := idx.ids[len(idx.ids)-1].MID
	item := p.wrapIndex(idx, 0)

	p.mu.Lock()
	defer p.mu.Unlock()

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

	p.readyToMerge[item.id] = item
	p.indexes = append(p.indexes, idx)
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

func (p *memIndexPool) replace(old []memIndexExt, newIndex *memIndex) {
	gen := 0
	for _, item := range old {
		gen += item.gen
	}
	merged := p.wrapIndex(newIndex, gen/len(old)+1)

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, eIndex := range old {
		delete(p.underMerging, eIndex.id)
	}
	p.readyToMerge[merged.id] = merged

	// Rebuild the index list for reading
	p.indexes = p.indexes[:0]
	p.indexes = slices.Grow(p.indexes, len(p.readyToMerge)+len(p.underMerging))

	for _, item := range p.readyToMerge {
		p.indexes = append(p.indexes, item.index) // add all ready indexes
	}
	for _, item := range p.underMerging {
		p.indexes = append(p.indexes, item.index) // add indexes currently being merged
	}

	go func() {
		// todo do we need wg here?
		for _, item := range old {
			item.index.Release()
		}
	}()
}

func (p *memIndexPool) Release() {
	p.mu.RLock()
	indexes := p.indexes
	p.mu.RUnlock()

	for _, idx := range indexes {
		idx.Release()
	}
}

func (p *memIndexPool) wrapIndex(index *memIndex, gen int) memIndexExt {
	return memIndexExt{
		id:    p.counter.Add(1),
		gen:   gen,
		index: index,
	}
}
