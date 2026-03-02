package active

import (
	"sync"

	"github.com/ozontech/seq-db/logger"
	"go.uber.org/zap"
)

// Tuning parameters for index merge strategy
const (
	maxGenerationBuckets = 32   // Maximum number of generation buckets used for grouping
	minMergeBatchSize    = 16   // Minimum batch size required to start a merge
	forceMergeThreshold  = 4096 // Merge all indexes if total count exceeds this limit
)

type WorkerLimiter interface {
	Acquire() // Blocks until a worker slot is available
	Release() // Frees a previously acquired slot
}

// mergeManager coordinates background merging of in-memory indexes
type mergeManager struct {
	mu sync.Mutex     // Protects internal state
	wg sync.WaitGroup // Tracks active merge jobs

	stopped   bool          // Indicates shutdown state
	indexPool *memIndexPool // Source of indexes to be merged

	mergeWorkers WorkerLimiter // Limits parallel merge execution
	mergeSignal  chan struct{} // Coalesced signal to trigger merge evaluation
}

// newMergeManager initializes merge manager and starts merge loop
func newMergeManager(indexes *memIndexPool, workerPool WorkerLimiter) *mergeManager {
	m := mergeManager{
		indexPool:    indexes,
		mergeWorkers: workerPool,
		mergeSignal:  make(chan struct{}, 1),
	}

	// Background goroutine responsible for scheduling merges
	go m.mergeLoop()

	return &m
}

// Stop gracefully stops the manager and waits for ongoing merges
func (m *mergeManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopped = true

	// Ensure all in-flight merges are completed
	m.wg.Wait()
	close(m.mergeSignal)
}

// ForceMergeAll performs full merge of all available indexes
func (m *mergeManager) ForceMergeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure no background merges are running
	m.wg.Wait()

	if batch := m.indexPool.ReadyToMerge(); len(batch) > 1 {
		logger.Debug("force merge all indexes", zap.Int("batch", len(batch)))
		m.indexPool.takeForMerge(batch)
		merged := mergeIndexes(unwrapIndexes(batch))
		m.indexPool.replace(batch, merged)
	}
}

// unwrapIndexes extracts raw memIndex pointers from wrappers
func unwrapIndexes(items []indexEntry) []*memIndex {
	result := make([]*memIndex, 0, len(items))
	for _, item := range items {
		result = append(result, item.index)
	}
	return result
}

// mergeLoop continuously reacts to merge signals and schedules work
func (m *mergeManager) mergeLoop() {
	for range m.mergeSignal {
		m.mergeWorkers.Acquire() // wait for a free worker

		m.mu.Lock()

		if m.stopped {
			m.mu.Unlock()
			m.mergeWorkers.Release()
			continue
		}

		// Decide which indexes are worth merging right now
		batch := selectMergeBatch(m.indexPool.ReadyToMerge(), minMergeBatchSize)
		if len(batch) == 0 {
			m.mu.Unlock()
			m.mergeWorkers.Release()
			continue
		}

		m.indexPool.takeForMerge(batch)
		m.wg.Add(1) // important to inc wg inside the lock
		m.mu.Unlock()

		logger.Debug("merge indexes", zap.Int("generation", batch[0].gen), zap.Int("size", len(batch)))

		go func(batch []indexEntry) {
			defer m.wg.Done()
			defer m.mergeWorkers.Release()

			merged := mergeIndexes(unwrapIndexes(batch))
			m.indexPool.replace(batch, merged)
			m.requestMerge() // re-check if further merges are possible
		}(batch)
	}
}

// requestMerge schedules a merge check if one is not already pending
func (m *mergeManager) requestMerge() {
	select {
	case m.mergeSignal <- struct{}{}:
	default:
		// Merge signal already pending; avoid redundant wakeups
	}
}

// selectMergeBatch chooses an optimal merge candidate batch
func selectMergeBatch(items []indexEntry, minBatchSize int) []indexEntry {
	if len(items) < minBatchSize {
		return nil
	}

	if len(items) > forceMergeThreshold {
		return items
	}

	batch := largestGenerationGroup(items)
	if len(batch) < minBatchSize {
		return nil
	}
	return batch
}

// largestGenerationGroup returns the biggest generation-aligned batch
func largestGenerationGroup(items []indexEntry) []indexEntry {
	maxGen := 0
	batches := make([][]indexEntry, maxGenerationBuckets+1)
	for _, item := range items {
		gen := min(maxGenerationBuckets, item.gen)
		batches[gen] = append(batches[gen], item)
		if len(batches[gen]) > len(batches[maxGen]) || len(batches[gen]) == len(batches[maxGen]) && gen > maxGen {
			maxGen = gen
		}
	}
	return batches[maxGen]
}
