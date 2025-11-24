package fracmanager

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/seq-db/frac"
)

// fractionRegistry manages fraction queues at different lifecycle stages.
// Tracks fractions through different stages: active → sealing → local → offloading → remote
// Ensures correct state transitions while maintaining chronological order.
// The entire structure is thread-safe due to internal synchronization.
// Lifecycle: Created once, persists through application lifetime.
type fractionRegistry struct {
	mu sync.RWMutex // Main mutex for protecting registry state

	// Lifecycle queues (FIFO order, oldest at lower indexes)
	sealing    []*syncActiveDestroyable
	locals     []*syncSealedDestroyable
	offloading []*syncSealedDestroyable
	remotes    []*syncRemoteDestroyable

	stats       registryStats // Size statistics for monitoring
	oldestTotal uint64        // Creation time of oldest fraction
	oldestLocal uint64        // Creation time of oldest fraction

	muAll  sync.RWMutex
	active *syncAppender
	all    *fractionsSnapshot
}

// NewFractionRegistry creates and initializes a new fraction registry instance.
// Populates the registry with existing active, local and remote fractions.
// Rebuilds the complete fractions list in chronological order.
func NewFractionRegistry(active *frac.Active, locals []*frac.Sealed, remotes []*frac.Remote) (*fractionRegistry, error) {
	if active == nil {
		return nil, errors.New("active fraction must be specified")
	}

	// Set current active fraction
	r := fractionRegistry{active: &syncAppender{instance: active}}

	// Initialize local sealed fractions
	for _, sealed := range locals {
		r.stats.locals.Add(sealed.Info())
		r.locals = append(r.locals, &syncSealedDestroyable{sealed: sealed})
	}

	// Initialize remote fractions
	for _, remote := range remotes {
		r.stats.remotes.Add(remote.Info())
		r.remotes = append(r.remotes, &syncRemoteDestroyable{remote: remote})
	}

	// Init oldest local value
	r.updateOldestLocal()

	// Rebuild complete fractions list in order
	r.rebuildSnapshot()

	return &r, nil
}

// Active returns the currently active writable fraction.
func (r *fractionRegistry) Active() *syncAppender {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.active
}

// FractionsSnapshot returns a read-only view of all fractions in creation order.
func (r *fractionRegistry) FractionsSnapshot() ([]frac.Fraction, ReleaseSnapshot) {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.all.Fractions()
}

// Stats returns current size statistics of the registry.
func (r *fractionRegistry) Stats() registryStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
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
func (r *fractionRegistry) RotateIfFull(maxSize uint64, newActive func() *frac.Active) (*syncActiveDestroyable, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active.instance.Info().DocsOnDisk <= maxSize {
		return nil, nil, nil
	}

	old := r.active
	sad := &syncActiveDestroyable{active: old.instance}
	r.sealing = append(r.sealing, sad)
	r.addActive(newActive())

	r.rebuildSnapshot()

	if err := old.Finalize(); err != nil {
		return nil, nil, err
	}

	curInfo := old.instance.Info()
	r.stats.sealing.Add(curInfo)

	wg := sync.WaitGroup{}
	wg.Add(1)
	// since old.WaitWriteIdle() can take some time, we don't want to do it under the lock
	// we will do it asynchronously in a goroutine.
	go func() {
		defer wg.Done()

		old.WaitWriteIdle() // can be long enough
		finalInfo := old.instance.Info()

		r.mu.Lock()
		defer r.mu.Unlock()

		// curInfo and finalInfo differ because while we are waiting for old.WaitWriteIdle(),
		// the latest data is being written to the active fraction index.
		r.stats.sealing.Sub(curInfo)
		r.stats.sealing.Add(finalInfo)
	}()

	return sad, wg.Wait, nil
}

// addActive sets a new active fraction and updates the complete fractions list.
func (r *fractionRegistry) addActive(a *frac.Active) {
	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.active = &syncAppender{instance: a}
}

// EvictLocal removes oldest local fractions to free disk space.
// If shouldOffload is true, moves fractions to offloading queue instead of deleting.
// Returns evicted fractions or error if insufficient space is released.
func (r *fractionRegistry) EvictLocal(shouldOffload bool, sizeLimit uint64) ([]*syncSealedDestroyable, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		count         int
		releasingSize uint64
	)

	// Calculate total used disk space
	totalUsedSize := r.stats.locals.totalSizeOnDisk +
		r.stats.sealing.totalSizeOnDisk +
		r.active.instance.Info().FullSize()

	// Determine how many oldest fractions need to be removed to meet size limit
	for _, item := range r.locals {
		if totalUsedSize-releasingSize <= sizeLimit {
			break
		}
		info := item.sealed.Info()
		releasingSize += info.FullSize()
		r.stats.locals.Sub(info)
		count++
	}

	// Check if enough space will be freed
	if totalUsedSize-releasingSize > sizeLimit {
		return nil, fmt.Errorf("insufficient space released: need to free %d more bytes "+
			"(total: %d, releasing: %d, limit: %d)",
			(totalUsedSize-releasingSize)-sizeLimit, totalUsedSize, releasingSize, sizeLimit)
	}

	// Extract fractions to evict
	evicted := r.locals[:count]
	r.locals = r.locals[count:]

	// Either offload or completely remove the fractions
	if shouldOffload {
		for _, item := range evicted {
			r.offloading = append(r.offloading, item)
			r.stats.offloading.Add(item.sealed.Info())
		}
	} else {
		r.rebuildSnapshot()
		r.updateOldestLocal() // Oldest local can be changed here
	}

	return evicted, nil
}

// EvictRemote removes oldest remote fractions based on retention policy.
// Fractions older than retention period are permanently deleted.
// Returns removed fractions or empty slice if nothing to remove.
func (r *fractionRegistry) EvictRemote(retention time.Duration) []*syncRemoteDestroyable {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	// Find fractions older than retention period
	for _, item := range r.remotes {
		info := item.remote.Info()
		if time.Since(time.UnixMilli(int64(info.CreationTime))) <= retention {
			break // Stop at first fraction within retention
		}
		r.stats.remotes.Sub(info)
		count++
	}

	evicted := r.remotes[:count]
	r.remotes = r.remotes[count:]
	r.rebuildSnapshot()

	return evicted
}

// PromoteToLocal moves fractions from sealing to local queue when sealing completes.
// Maintains strict ordering - younger fractions wait for older ones to seal first.
func (r *fractionRegistry) PromoteToLocal(active *syncActiveDestroyable, sealed *frac.Sealed) {
	r.mu.Lock()
	defer r.mu.Unlock()

	active.sealed = sealed

	promotedCount := 0
	// Process sealing queue in order, promoting completed fractions
	for _, item := range r.sealing {
		if item.sealed == nil {
			break // Maintain order - wait for previous fractions to complete
		}
		promotedCount++
		r.locals = append(r.locals, &syncSealedDestroyable{sealed: item.sealed})
		r.stats.locals.Add(item.sealed.Info())
		r.stats.sealing.Sub(item.active.Info())
	}

	if promotedCount > 0 {
		// Remove promoted fractions from sealing queue and rebuild snapshot
		r.sealing = r.sealing[promotedCount:]
		r.rebuildSnapshot()
	}
}

// PromoteToRemote moves fractions from offloading to remote queue when offloading completes.
// Special case: Handles fractions that don't require offloading (remote == nil).
// Maintains strict ordering - younger fractions wait for older ones to offload.
func (r *fractionRegistry) PromoteToRemote(sealed *syncSealedDestroyable, remote *frac.Remote) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sealed.remote = remote

	// Special case: remote == nil means fraction doesn't require offloading
	if remote == nil {
		r.removeFromOffloading(sealed)
	}

	promotedCount := 0
	// Process offloading queue in order, promoting completed fractions
	for _, item := range r.offloading {
		if item.remote == nil {
			break // Maintain order - wait for previous fractions to complete
		}
		promotedCount++
		r.remotes = append(r.remotes, &syncRemoteDestroyable{remote: item.remote})

		r.stats.remotes.Add(item.remote.Info())
		r.stats.offloading.Sub(item.sealed.Info())
	}
	if promotedCount > 0 {
		// Remove promoted fractions from offloading queue
		r.offloading = r.offloading[promotedCount:]
		r.updateOldestLocal()
		r.rebuildSnapshot()
	}
}

// removeFromOffloading removes a specific fraction from offloading queue.
// O(n) operation that rebuilds the all fractions list.
func (r *fractionRegistry) removeFromOffloading(sealed *syncSealedDestroyable) {
	count := 0
	// Filter out the target fraction
	for _, item := range r.offloading {
		if sealed.sealed != item.sealed {
			r.offloading[count] = item
			count++
		}
	}
	r.offloading = r.offloading[:count]
	r.stats.offloading.Sub(sealed.sealed.Info())

	// Oldest local can be changed here
	r.updateOldestLocal()

	// Rebuild complete list since we modified the middle of the queue
	r.rebuildSnapshot()
}

// rebuildSnapshot reconstructs the all fractions list in correct chronological order.
// Order: remote (oldest) → offloading → local → sealing → active (newest)
// Expensive O(n) operation used when direct list modification is insufficient.
func (r *fractionRegistry) rebuildSnapshot() {
	all := newFractionsSnapshot(r.all.Len())

	// Collect fractions in correct chronological order: from oldest (remote) to newest (active)
	for _, remote := range r.remotes {
		all.AppendRemote(remote)
	}
	for _, offloaded := range r.offloading {
		all.AppendSealed(offloaded)
	}
	for _, sealed := range r.locals {
		all.AppendSealed(sealed)
	}
	for _, active := range r.sealing {
		all.AppendActive(active)
	}

	// we wrap the current active fraction in syncActiveDestroyable solely to comply with the API,
	// since it is never returned for Destroy and we don't store this instance.
	all.AppendActive(&syncActiveDestroyable{active: r.active.instance})

	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.all = all
	r.updateOldestTotal()
}

// updateOldestTotal recalculates the creation time of the oldest fraction.
// Called after modifications of the complete fractions list.
func (r *fractionRegistry) updateOldestTotal() {
	r.oldestTotal = r.all.f[0].Info().CreationTime
}

// updateOldestLocal recalculates the creation time of the oldest local fraction.
// Called after modifications of the local fractions list.
func (r *fractionRegistry) updateOldestLocal() {
	if len(r.offloading) > 0 {
		r.oldestLocal = r.offloading[0].sealed.Info().CreationTime
	} else if len(r.locals) > 0 {
		r.oldestLocal = r.locals[0].sealed.Info().CreationTime
	} else if len(r.sealing) > 0 {
		r.oldestLocal = r.sealing[0].active.Info().CreationTime
	} else {
		r.oldestLocal = r.active.instance.Info().CreationTime
	}
}
