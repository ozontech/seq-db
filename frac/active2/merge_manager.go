package active2

import (
	"sync"

	"github.com/ozontech/seq-db/logger"
	"go.uber.org/zap"
)

const (
	maxGenerations      = 32
	minIndexesToMerge   = 16   // minimum number of indexes to trigger merge
	forceMergeThreshold = 4096 // index count threshold for forced merge
)

type Semaphore interface {
	Acquire()
	Release()
	Capacity() int
}

// MergeManager manages in-memory index collection and merging
type MergeManager struct {
	mu sync.Mutex
	wg sync.WaitGroup

	stopped bool
	indexes *memIndexPool

	workerPool Semaphore
	mergeCh    chan struct{} // channel to trigger merge process
}

// NewMergeManager creates a new index manager
func NewMergeManager(indexes *memIndexPool, workerPool Semaphore) *MergeManager {
	m := MergeManager{
		indexes:    indexes,
		workerPool: workerPool,
		mergeCh:    make(chan struct{}, 1),
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
		logger.Debug("merge all mini-indexes", zap.Int("batch", len(toMerge)))
		m.indexes.markAsMerging(toMerge)
		merged := mergeIndexes(extractIndexes(toMerge))
		m.indexes.replace(toMerge, merged)
	}
}

func extractIndexes(items []memIndexExt) []*memIndex {
	result := make([]*memIndex, 0, len(items))
	for _, item := range items {
		result = append(result, item.index)
	}
	return result
}

func (m *MergeManager) mergeScheduler() {
	for range m.mergeCh {
		m.workerPool.Acquire() // wait for a free worker

		m.mu.Lock()

		if m.stopped {
			m.mu.Unlock()
			m.workerPool.Release()
			continue
		}

		batch := pickToMerge(m.indexes.ReadyToMerge(), minIndexesToMerge)
		if len(batch) == 0 {
			m.mu.Unlock()
			m.workerPool.Release()
			continue
		}

		m.indexes.markAsMerging(batch)
		m.wg.Add(1) // important to inc wg inside the lock
		m.mu.Unlock()

		logger.Debug("merge indexes", zap.Int("gen", batch[0].gen), zap.Int("batch", len(batch)))

		go func() {
			merged := mergeIndexes(extractIndexes(batch))
			m.workerPool.Release()
			m.indexes.replace(batch, merged)
			m.triggerMerge() // check if new merge is needed
			m.wg.Done()
		}()
	}
}

func (m *MergeManager) triggerMerge() {
	select {
	case m.mergeCh <- struct{}{}:
	default:
		// Trigger already set, no need for additional notification
	}
}

func pickToMerge(items []memIndexExt, minBatchSize int) []memIndexExt {
	if len(items) < minBatchSize {
		return nil
	}

	if len(items) > forceMergeThreshold {
		return items
	}

	batch := largestBatch(items)
	if len(batch) < minBatchSize {
		return nil
	}
	return batch
}

func largestBatch(items []memIndexExt) []memIndexExt {
	maxGen := 0
	batches := make([][]memIndexExt, maxGenerations)
	for _, item := range items {
		gen := min(maxGenerations, item.gen)
		batches[gen] = append(batches[gen], item)
		if len(batches[gen]) > len(batches[maxGen]) || len(batches[gen]) == len(batches[maxGen]) && gen > maxGen {
			maxGen = gen
		}
	}
	return batches[maxGen]
}
