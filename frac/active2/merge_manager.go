package active2

import (
	"sync"
)

const (
	minIndexesToMerge    = 4    // minimum number of indexes to trigger merge
	forceMergeThreshold  = 64   // index count threshold for forced merge
	tierSizeDeltaPercent = 10   // percentage difference between size tiers
	firstTierMaxSizeKb   = 8    // maximum size of the first tier
	maxTierCount         = 1000 // todo  maximum number of size tiers allowed
	bucketSizePercent    = 50   // percentage difference between size buckets
)

// MergeManager manages in-memory index collection and merging
type MergeManager struct {
	mu sync.Mutex
	wg sync.WaitGroup

	stopped bool
	indexes *memIndexPool

	workers chan struct{} // semaphore to limit concurrent merge operations
	mergeCh chan struct{} // channel to trigger merge process
}

// newMergeManager creates a new index manager
func newMergeManager(maxConcurrentMerges int) *MergeManager {
	m := MergeManager{
		workers: make(chan struct{}, maxConcurrentMerges),
		mergeCh: make(chan struct{}, 1),
		indexes: newIndexPool(),
	}

	// Start background goroutine for merge scheduling
	go m.mergeScheduler()

	return &m
}

// Stop shuts down the index manager and waits for current operations to complete
func (m *MergeManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopped = true

	// Wait for all current merge operations to complete
	m.wg.Wait()
	close(m.mergeCh)
}

// MergeAll performs full merge of all available indexes
func (m *MergeManager) MergeAll() *memIndex {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.wg.Wait()

	if len(m.indexes.indexes) == 1 {
		return m.indexes.indexes[0]
	}

	// todo обработать случай когда нет индексов вообще

	indexesToMerge := m.indexes.ReadyToMerge()
	m.indexes.markAsMerging(indexesToMerge)
	mergedIndex := mergeIndexes(extractIndexes(indexesToMerge))
	m.indexes.replace(indexesToMerge, mergedIndex)

	return mergedIndex
}

func extractIndexes(metadataList []memIndexExt) []*memIndex {
	result := make([]*memIndex, 0, len(metadataList))
	for _, metadata := range metadataList {
		result = append(result, metadata.index)
	}
	return result
}

func (m *MergeManager) Indexes() []*memIndex {
	return m.indexes.Indexes()
}

func (m *MergeManager) Add(index *memIndex) {
	m.indexes.Add(index)
	m.triggerMerge()
}

// prepareForMerging prepares index groups for merging
func (m *MergeManager) prepareForMerging() [][]memIndexExt {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return nil
	}

	mergeCandidates := selectForMerge(m.indexes.ReadyToMerge(), minIndexesToMerge)

	for i, candidateGroup := range mergeCandidates {
		if !m.acquireWorker() { // no free workers
			mergeCandidates = mergeCandidates[:i] // truncate unprocessable tail
			break
		}
		m.indexes.markAsMerging(candidateGroup)
	}

	// Important: call Add() inside lock to prevent races during shutdown
	m.wg.Add(len(mergeCandidates))

	return mergeCandidates
}

func (m *MergeManager) mergeScheduler() {
	for range m.mergeCh {
		for {
			preparedGroups := m.prepareForMerging()
			if len(preparedGroups) == 0 {
				break
			}

			for _, toMerge := range preparedGroups {
				go func() {
					mergedIndex := mergeIndexes(extractIndexes(toMerge))
					m.indexes.replace(toMerge, mergedIndex)
					m.releaseWorker()
					m.triggerMerge() // check if new merge is needed
					m.wg.Done()
				}()
			}
		}
	}
}

func (m *MergeManager) acquireWorker() bool {
	select {
	case m.workers <- struct{}{}:
		return true
	default:
		return false
	}
}

func (m *MergeManager) releaseWorker() {
	<-m.workers
}

func (m *MergeManager) triggerMerge() {
	select {
	case m.mergeCh <- struct{}{}:
	default:
		// Trigger already set, no need for additional notification
	}
}
