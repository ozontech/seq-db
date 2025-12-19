package active2

import (
	"sync"
)

const (
	minIndexesToMerge    = 4  // minimum number of indexes to trigger merge
	forceMergeThreshold  = 64 // index count threshold for forced merge
	firstTierMaxSizeKb   = 8  // maximum size of the first tier
	maxTierCount         = 64 // maximum number of size tiers allowed
	tierSizeDeltaPercent = 25 // percentage difference between size tiers
	bucketSizePercent    = 50 // percentage difference between size buckets
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

// NewMergeManager creates a new index manager
func NewMergeManager(indexes *memIndexPool, maxConcurrentMerges int) *MergeManager {
	m := MergeManager{
		indexes: indexes,
		workers: make(chan struct{}, maxConcurrentMerges),
		mergeCh: make(chan struct{}, 1),
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
func (m *MergeManager) MergeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.wg.Wait()

	if toMerge := m.indexes.ReadyToMerge(); len(toMerge) > 1 {
		m.indexes.markAsMerging(toMerge)
		merged := mergeIndexes(extractIndexes(toMerge))
		m.indexes.replace(toMerge, merged)
	}
}

func extractIndexes(indexesExt []memIndexExt) []*memIndex {
	result := make([]*memIndex, 0, len(indexesExt))
	for _, eIdx := range indexesExt {
		result = append(result, eIdx.index)
	}
	return result
}

// prepareForMerging prepares index groups for merging
func (m *MergeManager) prepareForMerging() [][]memIndexExt {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return nil
	}

	mergeCandidates := pickMergeCandidates(m.indexes.ReadyToMerge(), minIndexesToMerge)

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
