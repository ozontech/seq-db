package fracmanager

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

// fractionRegistry manages fraction queues at different lifecycle stages.
// Tracks fractions through different stages: active → sealing → sealed → offloading → remote
// Ensures correct state transitions while maintaining chronological order.
// The entire structure is thread-safe due to internal synchronization.
// Lifecycle: created once, persists through application lifetime.
type fractionRegistry struct {
	mu sync.RWMutex // main mutex for protecting registry state

	// lifecycle queues (FIFO order, oldest at lower indexes)
	sealing    []*activeProxy // fractions being sealed (0-5 typical)
	sealed     []*sealedProxy // local sealed fractions (can be thousands)
	offloading []*sealedProxy // fractions being offloaded (0-5 typical)
	remotes    []*remoteProxy // offloaded fractions (can be thousands)

	stats       registryStats // size statistics for monitoring
	oldestTotal uint64        // creation time of oldest fraction in all list including remote
	oldestLocal uint64        // creation time of oldest fraction in local or offloading queues

	muAll  sync.RWMutex    // protects active, all, and oldestTotal fields
	active *activeProxy    // currently active writable fraction
	all    []frac.Fraction // all fractions in creation order (read-only view)
}

// NewFractionRegistry creates and initializes a new fraction registry instance.
// Populates the registry with existing active, sealed and remote fractions.
// Rebuilds the complete fractions list in chronological order.
func NewFractionRegistry(active *frac.Active, sealed []*frac.Sealed, remotes []*frac.Remote) (*fractionRegistry, error) {
	if active == nil {
		return nil, errors.New("active fraction must be specified")
	}

	r := fractionRegistry{
		active: &activeProxy{
			proxy:    &fractionProxy{impl: active},
			instance: active,
		},
	}

	// initialize local sealed fractions
	for _, sealed := range sealed {
		r.stats.sealed.Add(sealed.Info())
		r.sealed = append(r.sealed, &sealedProxy{
			proxy:    &fractionProxy{impl: sealed},
			instance: sealed,
		})
	}

	// initialize remote fractions
	for _, remote := range remotes {
		r.stats.remotes.Add(remote.Info())
		r.remotes = append(r.remotes, &remoteProxy{
			proxy:    &fractionProxy{impl: remote},
			instance: remote,
		})
	}

	r.updateOldestLocal()
	r.rebuildAllFractions()

	return &r, nil
}

// Active returns the currently active writable fraction.
func (r *fractionRegistry) Active() *activeProxy {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.active
}

// AllFractions returns a read-only view of all fractions in creation order.
func (r *fractionRegistry) AllFractions() []frac.Fraction {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.all
}

// Stats returns current size statistics of the registry.
func (r *fractionRegistry) Stats() registryStats {
	r.mu.RLock()
	s := r.stats
	i := r.active.instance.Info()
	r.mu.RUnlock()

	s.active.Set(i)
	return s
}

// OldestTotal returns the creation time of the oldest fraction in the registry.
func (r *fractionRegistry) OldestTotal() uint64 {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.oldestTotal
}

// OldestLocal returns the creation time of the oldest local fraction in the registry.
func (r *fractionRegistry) OldestLocal() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.oldestLocal
}

// RotateIfFull completes the current active fraction and starts a new one.
// Moves previous active fraction to sealing queue.
// Updates statistics and maintains chronological order.
// Should be called when creating a new fraction.
func (r *fractionRegistry) RotateIfFull(maxSize uint64, newActive func() *activeProxy) (*activeProxy, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active.instance.Info().DocsOnDisk <= maxSize {
		return nil, nil, nil
	}

	old := r.active
	r.sealing = append(r.sealing, old)
	r.addActive(newActive())

	if err := old.Finalize(); err != nil {
		return old, nil, err
	}

	curInfo := old.instance.Info()
	r.stats.sealing.Add(curInfo)

	r.active.Suspend(old.Suspended())

	wg := sync.WaitGroup{}
	wg.Add(1)
	// since old.WaitWriteIdle() can take some time, we don't want to do it under the lock
	// we will do it asynchronously in a goroutine
	go func() {
		defer wg.Done()

		old.WaitWriteIdle() // can be long enough
		finalInfo := old.instance.Info()

		r.mu.Lock()
		defer r.mu.Unlock()

		// curInfo and finalInfo differ because while we are waiting for old.WaitWriteIdle(),
		// the latest data is being written to the active fraction index
		r.stats.sealing.Sub(curInfo)
		r.stats.sealing.Add(finalInfo)
	}()

	return old, wg.Wait, nil
}

func (r *fractionRegistry) SuspendIfOverCapacity(maxQueue, maxSize uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	suspended := r.active.Suspended()

	if maxQueue > 0 && r.stats.sealing.count >= int(maxQueue) {
		if !suspended {
			logger.Warn("switching to read-only mode",
				zap.String("reason", "sealing queue size exceeded"),
				zap.Uint64("limit", maxQueue),
				zap.Int("queue_size", r.stats.sealing.count))
			r.active.Suspend(true)
		}
		return
	}

	du := r.diskUsage()

	if maxSize > 0 && du > maxSize {
		if !suspended {
			logger.Warn("switching to read-only mode",
				zap.String("reason", "occupied space limit exceeded"),
				zap.Float64("queue_size_limit_gb", util.Float64ToPrec(util.SizeToUnit(maxSize, "gb"), 2)),
				zap.Float64("occupied_space_gb", util.Float64ToPrec(util.SizeToUnit(du, "gb"), 2)))
			r.active.Suspend(true)
		}
		return
	}

	if suspended {
		logger.Warn("switching to write mode",
			zap.Float64("queue_size_limit_gb", util.Float64ToPrec(util.SizeToUnit(maxSize, "gb"), 2)),
			zap.Float64("occupied_space_gb", util.Float64ToPrec(util.SizeToUnit(du, "gb"), 2)),
			zap.Uint64("sealing_queue_size_limit", maxQueue),
			zap.Int("queue_size", r.stats.sealing.count))
		r.active.Suspend(false)
	}
}

func (r *fractionRegistry) diskUsage() uint64 {
	return r.active.instance.Info().FullSize() +
		r.stats.sealed.totalSizeOnDisk +
		r.stats.sealing.totalSizeOnDisk +
		r.stats.offloading.totalSizeOnDisk
}

// addActive sets a new active fraction and updates the complete fractions list.
func (r *fractionRegistry) addActive(a *activeProxy) {
	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.active = a
	r.all = append(r.all, a.proxy)
}

// trimAll removes the oldest fractions from the complete fractions list.
// Used when fractions are evicted or deleted from the system.
func (r *fractionRegistry) trimAll(count int) {
	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.all = r.all[count:]
	r.updateOldestTotal()
}

// EvictLocal removes oldest local fractions to free disk space.
// If shouldOffload is true, moves fractions to offloading queue instead of deleting.
// Returns evicted fractions or error if insufficient space is released.
func (r *fractionRegistry) EvictLocal(shouldOffload bool, sizeLimit uint64) ([]*sealedProxy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		count         int
		releasingSize uint64
	)

	// calculate total used disk space
	totalUsedSize := r.stats.sealed.totalSizeOnDisk +
		r.stats.sealing.totalSizeOnDisk +
		r.active.instance.Info().FullSize()

	// determine how many oldest fractions need to be removed to meet size limit
	for _, item := range r.sealed {
		if totalUsedSize-releasingSize <= sizeLimit {
			break
		}
		info := item.instance.Info()
		releasingSize += info.FullSize()
		r.stats.sealed.Sub(info)
		count++
	}

	// check if enough space will be freed
	if totalUsedSize-releasingSize > sizeLimit {
		return nil, fmt.Errorf("insufficient space released: need to free %d more bytes "+
			"(total: %d, releasing: %d, limit: %d)",
			(totalUsedSize-releasingSize)-sizeLimit, totalUsedSize, releasingSize, sizeLimit)
	}

	// extract fractions to evict
	evicted := r.sealed[:count]
	r.sealed = r.sealed[count:]

	// either offload or completely remove the fractions
	if shouldOffload {
		for _, item := range evicted {
			r.offloading = append(r.offloading, item)
			r.stats.offloading.Add(item.instance.Info())
		}
	} else {
		r.trimAll(count)      // permanently remove
		r.updateOldestLocal() // oldest local can be changed here
	}

	return evicted, nil
}

// EvictRemote removes oldest remote fractions based on retention policy.
// Fractions older than retention period are permanently deleted.
// Returns removed fractions or empty slice if nothing to remove.
func (r *fractionRegistry) EvictRemote(retention time.Duration) []*remoteProxy {
	if retention == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	// find fractions older than retention period
	for _, item := range r.remotes {
		info := item.instance.Info()
		if time.Since(time.UnixMilli(int64(info.CreationTime))) <= retention {
			break // stop at first fraction within retention
		}
		r.stats.remotes.Sub(info)
		count++
	}

	evicted := r.remotes[:count]
	r.remotes = r.remotes[count:]
	r.trimAll(count) // remove from complete list

	return evicted
}

// EvictOverflowed removes oldest fractions from offloading queue when it exceeds size limit.
// Selects fractions that haven't finished offloading yet to minimize data loss.
// Used when offloading queue grows too large due to slow remote storage performance.
func (r *fractionRegistry) EvictOverflowed(sizeLimit uint64) []*sealedProxy {
	if sizeLimit == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Fast path: skip processing if within size limits
	if r.stats.offloading.totalSizeOnDisk <= sizeLimit {
		return nil
	}

	count := 0
	evicted := []*sealedProxy{}
	// filter fractions
	for _, item := range r.offloading {
		// keep items that are within limits or already offloaded
		if r.stats.offloading.totalSizeOnDisk <= sizeLimit || item.remote != nil {
			r.offloading[count] = item
			count++
			continue
		}
		evicted = append(evicted, item)
		r.stats.offloading.Sub(item.instance.Info())
	}

	r.offloading = r.offloading[:count]
	r.rebuildAllFractions()

	return evicted
}

// PromoteToSealed moves fractions from sealing to local queue when sealing completes.
// Maintains strict ordering - younger fractions wait for older ones to seal first.
func (r *fractionRegistry) PromoteToSealed(active *activeProxy, sealed *frac.Sealed) {
	r.mu.Lock()
	defer r.mu.Unlock()

	active.sealed = sealed

	promotedCount := 0
	// process sealing queue in order, promoting completed fractions
	for _, item := range r.sealing {
		if item.sealed == nil {
			break // maintain order - wait for previous fractions to complete
		}
		promotedCount++
		r.sealed = append(r.sealed, &sealedProxy{
			proxy:    item.proxy,
			instance: item.sealed,
		})
		r.stats.sealed.Add(item.sealed.Info())
		r.stats.sealing.Sub(item.instance.Info())
	}

	// remove promoted fractions from sealing queue
	r.sealing = r.sealing[promotedCount:]
}

// PromoteToRemote moves fractions from offloading to remote queue when offloading completes.
// Special case: handles fractions that don't require offloading (remote == nil).
// Maintains strict ordering - younger fractions wait for older ones to offload.
func (r *fractionRegistry) PromoteToRemote(sealed *sealedProxy, remote *frac.Remote) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sealed.remote = remote

	// special case: remote == nil means fraction doesn't require offloading
	if remote == nil {
		r.removeFromOffloading(sealed)
	}

	promotedCount := 0
	// process offloading queue in order, promoting completed fractions
	for _, item := range r.offloading {
		if item.remote == nil {
			break // maintain order - wait for previous fractions to complete
		}
		promotedCount++
		r.remotes = append(r.remotes, &remoteProxy{
			proxy:    item.proxy,
			instance: item.remote,
		})

		r.stats.remotes.Add(item.remote.Info())
		r.stats.offloading.Sub(item.instance.Info())
	}
	if promotedCount > 0 {
		// remove promoted fractions from offloading queue
		r.offloading = r.offloading[promotedCount:]
		r.updateOldestLocal()
	}
}

// removeFromOffloading removes a specific fraction from offloading queue.
// O(n) operation that rebuilds the all fractions list.
func (r *fractionRegistry) removeFromOffloading(sealed *sealedProxy) {
	count := 0
	// filter out the target fraction
	for _, item := range r.offloading {
		if sealed != item {
			r.offloading[count] = item
			count++
		}
	}

	if count == len(r.offloading) { // not found to remove (can be removed earlier in EvictOverflowed)
		return
	}

	r.offloading = r.offloading[:count]
	r.stats.offloading.Sub(sealed.instance.Info())

	// oldest local can be changed here
	r.updateOldestLocal()

	// rebuild complete list since we modified the middle of the queue
	r.rebuildAllFractions()
}

// rebuildAllFractions reconstructs the all fractions list in correct chronological order.
// Order: remote (oldest) → offloading → sealed → sealing → active (newest)
// Expensive O(n) operation used when direct list modification is insufficient.
func (r *fractionRegistry) rebuildAllFractions() {
	all := make([]frac.Fraction, 0, len(r.all))

	// collect fractions in correct chronological order: from oldest (remote) to newest (active)
	for _, remote := range r.remotes {
		all = append(all, remote.proxy)
	}
	for _, offloaded := range r.offloading {
		all = append(all, offloaded.proxy)
	}
	for _, sealed := range r.sealed {
		all = append(all, sealed.proxy)
	}
	for _, active := range r.sealing {
		all = append(all, active.proxy)
	}
	all = append(all, r.active.proxy)

	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.all = all
	r.updateOldestTotal()
}

// updateOldestTotal recalculates the creation time of the oldest fraction.
// Called after modifications of the complete fractions list.
func (r *fractionRegistry) updateOldestTotal() {
	r.oldestTotal = r.all[0].Info().CreationTime
}

// updateOldestLocal recalculates the creation time of the oldest local fraction.
// Priority order: offloading queue → sealed queue → sealing queue → active fraction.
// Called after modifications
func (r *fractionRegistry) updateOldestLocal() {
	if len(r.offloading) > 0 {
		r.oldestLocal = r.offloading[0].proxy.Info().CreationTime
	} else if len(r.sealed) > 0 {
		r.oldestLocal = r.sealed[0].proxy.Info().CreationTime
	} else if len(r.sealing) > 0 {
		r.oldestLocal = r.sealing[0].proxy.Info().CreationTime
	} else {
		r.oldestLocal = r.active.proxy.Info().CreationTime
	}
}
