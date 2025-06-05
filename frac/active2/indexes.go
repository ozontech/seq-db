package active2

import (
	"slices"
	"sync"
	"sync/atomic"
)

type indexItem struct {
	id    uint64
	tier  int
	index *memIndex
}

type Indexes struct {
	sem     chan struct{}
	merge   chan struct{}
	counter atomic.Uint64
	tiers   sizeTiers

	mu         sync.RWMutex
	unoccupied map[uint64]indexItem // map of indexes ready for merge
	processing map[uint64]indexItem // map of indexes taken for merging
	indexesBuf []*memIndex

	mergingMu sync.Mutex
	stopped   bool

	iMu     sync.RWMutex
	indexes []*memIndex // list of indexes for read - it is copy of `waiting` + `merging` with its own lock

	wg sync.WaitGroup
}

const (
	minToMerge           = 4   // minimum number of indexes required for merging
	mergeForcedThreshold = 100 // the number of indexes at which it is necessary to merge some of them

	tierDeltaPercent = 10   // different between size tiers
	tierFirstMax     = 100  // max size in first tier
	tierMax          = 1000 // maximum number of size tiers allowed

	bucketPercent = 50 // different between size buckets
)

func newIndexes(sem chan struct{}) *Indexes {
	indexes := Indexes{
		sem:        sem,
		merge:      make(chan struct{}, 1),
		unoccupied: make(map[uint64]indexItem),
		processing: make(map[uint64]indexItem),
		tiers:      newSizeTiers(tierFirstMax, tierMax, tierDeltaPercent),
	}
	go indexes.mergeLoop()
	return &indexes
}

func (indexes *Indexes) StopMergeLoop() {
	indexes.mergingMu.Lock()
	defer indexes.mergingMu.Unlock()

	indexes.stopped = true

	indexes.wg.Wait() // waiting for the completion of all current merger processes
	close(indexes.merge)
}

func (indexes *Indexes) MergeAll() *memIndex {
	indexes.mergingMu.Lock()
	defer indexes.mergingMu.Unlock()

	indexes.wg.Wait() // waiting for the completion of all current merger processes

	merging := indexes.allUnoccupied()
	merged := MergeIndexes(unwrapIndexes(merging))
	indexes.replace(merging, indexes.wrapIndex(merged))

	return merged
}

func (indexes *Indexes) wrapIndex(index *memIndex) indexItem {
	return indexItem{
		id:    indexes.counter.Add(1),
		tier:  indexes.tiers.Calc(index.docsCount),
		index: index,
	}
}

func unwrapIndexes(items []indexItem) []*memIndex {
	res := make([]*memIndex, 0, len(items))
	for _, item := range items {
		res = append(res, item.index)
	}
	return res
}

func (indexes *Indexes) Add(index *memIndex) {
	item := indexes.wrapIndex(index)

	indexes.mu.Lock()
	indexes.unoccupied[item.id] = item
	{
		indexes.iMu.Lock()
		indexes.indexes = append(indexes.indexes, index)
		indexes.iMu.Unlock()
	}
	indexes.mu.Unlock()

	indexes.launchMerge()
}

func (indexes *Indexes) Indexes() []*memIndex {
	indexes.iMu.RLock()
	defer indexes.iMu.RUnlock()

	return indexes.indexes
}

func (indexes *Indexes) allUnoccupied() []indexItem {
	indexes.mu.RLock()
	defer indexes.mu.RUnlock()

	items := make([]indexItem, 0, len(indexes.unoccupied))
	for _, item := range indexes.unoccupied {
		items = append(items, item)
	}
	return items
}

func (indexes *Indexes) markAsProcessing(items []indexItem) {
	indexes.mu.Lock()
	defer indexes.mu.Unlock()

	for _, item := range items {
		delete(indexes.unoccupied, item.id)
		indexes.processing[item.id] = item
	}
}

func (indexes *Indexes) prepareToMerge() [][]indexItem {
	indexes.mergingMu.Lock()
	defer indexes.mergingMu.Unlock()

	if indexes.stopped {
		return nil
	}

	suitable := SelectForMerge(indexes.allUnoccupied(), minToMerge)
	for n, items := range suitable {
		if !indexes.acquireWorker() { // there are no free workers
			suitable = suitable[:n] // so we cut off the tail that we can't process right now
			break
		}
		indexes.markAsProcessing(items)
	}

	// it is important to wg.Add() inside the lock otherwise indexes.wg.Wait() and close(indexes.merge)
	// will be called before wg.Add() and indexes.mergeNotify() (see indexes.Stop())
	indexes.wg.Add(len(suitable))

	return suitable
}

func (indexes *Indexes) mergeLoop() {
	for range indexes.merge {
		for {
			prepared := indexes.prepareToMerge()
			if len(prepared) == 0 {
				break
			}
			for _, merging := range prepared {
				go func(merging []indexItem) {
					merged := MergeIndexes(unwrapIndexes(merging))
					indexes.replace(merging, indexes.wrapIndex(merged))

					indexes.releaseWorker()
					indexes.launchMerge()
					indexes.wg.Done()
				}(merging)
			}
		}
	}
}

func (indexes *Indexes) acquireWorker() bool {
	select {
	case indexes.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (indexes *Indexes) releaseWorker() {
	<-indexes.sem
}

func (indexes *Indexes) launchMerge() {
	select {
	case indexes.merge <- struct{}{}:
	default:
	}
	return
}

func (indexes *Indexes) replace(processed []indexItem, merged indexItem) {
	indexes.mu.Lock()
	defer indexes.mu.Unlock()

	// replace
	for _, index := range processed {
		delete(indexes.processing, index.id)
	}
	indexes.unoccupied[merged.id] = merged

	// rebuild indexes.indexes
	indexes.indexesBuf = indexes.indexesBuf[:0]
	indexes.indexesBuf = slices.Grow(indexes.indexesBuf, len(indexes.unoccupied)+len(indexes.processing))
	for _, index := range indexes.unoccupied {
		indexes.indexesBuf = append(indexes.indexesBuf, index.index)
	}
	for _, index := range indexes.processing {
		indexes.indexesBuf = append(indexes.indexesBuf, index.index)
	}

	// swap indexes.indexes
	indexes.iMu.Lock()
	indexes.indexesBuf, indexes.indexes = indexes.indexes, indexes.indexesBuf
	indexes.iMu.Unlock()

	// avoid old indexes leak
	clear(indexes.indexesBuf)
}
