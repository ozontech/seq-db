package fracmanager

import (
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
	sealing    []*activeProxy // Fractions being sealed (0-5 typical)
	locals     []*sealedProxy // Local sealed fractions (can be thousands)
	offloading []*sealedProxy // Fractions being offloaded (0-5 typical)
	remotes    []*remoteProxy // Offloaded fractions (can be thousands)

	inFlight map[string]struct{} // Set of fraction names currently being offloaded

	stats  registryStats // Size statistics for monitoring
	oldest uint64        // Creation time of oldest fraction

	muAll  sync.RWMutex    // Mutex specifically for all fractions list
	active *activeProxy    // Currently active writable fraction
	all    []frac.Fraction // All fractions in creation order (read-only view)
}

// NewFractionRegistry creates and initializes a new fraction registry instance.
// Populates the registry with existing remote, local, and active fractions.
// Rebuilds the complete fractions list in chronological order.
func NewFractionRegistry(remotes []*frac.Remote, locals []*frac.Sealed, active *frac.Active) *fractionRegistry {
	r := fractionRegistry{
		inFlight: map[string]struct{}{},
	}

	// Initialize local sealed fractions
	for _, sealed := range locals {
		r.stats.locals.Add(sealed.Info())
		r.locals = append(r.locals, &sealedProxy{
			proxy:    &fractionProxy{impl: sealed},
			instance: sealed,
		})
	}

	// Initialize remote fractions
	for _, remote := range remotes {
		r.stats.remotes.Add(remote.Info())
		r.remotes = append(r.remotes, &remoteProxy{
			proxy:    &fractionProxy{impl: remote},
			instance: remote,
		})
	}

	// Set current active fraction
	r.active = &activeProxy{
		proxy:    &fractionProxy{impl: active},
		instance: active,
	}

	// Rebuild complete fractions list in order
	r.rebuildAllFractions()

	return &r
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
	defer r.mu.RUnlock()
	return r.stats
}

// Oldest returns the creation time of the oldest fraction in the registry.
func (r *fractionRegistry) Oldest() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.oldest
}

// Rotate completes the current active fraction and starts a new one.
// Moves previous active fraction to sealing queue.
// Updates statistics and maintains chronological order.
// Should be called when creating a new fraction.
func (r *fractionRegistry) Rotate(newActive *activeProxy) *activeProxy {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev := r.active
	r.sealing = append(r.sealing, prev)
	// r.stats.sealing.Add(prev.instance.Info())

	r.addNewActive(newActive)

	go func() {
		if err := prev.Freeze(); err != nil {
			// return err
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.stats.sealing.Add(prev.instance.Info())

	}()

	return prev
}

// addNewActive sets a new active fraction and updates the complete fractions list.
func (r *fractionRegistry) addNewActive(newActive *activeProxy) {
	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.active = newActive
	r.all = append(r.all, newActive.proxy)
	r.updateOldest()
}

// trimAll removes the oldest fractions from the complete fractions list.
// Used when fractions are evicted or deleted from the system.
func (r *fractionRegistry) trimAll(count int) {
	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.all = r.all[count:]
	r.updateOldest()
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

	// Calculate total used disk space
	totalUsedSize := r.stats.locals.totalSizeOnDisk +
		r.stats.sealing.totalSizeOnDisk +
		r.active.instance.Info().FullSize()

	// Determine how many oldest fractions need to be removed to meet size limit
	for _, item := range r.locals {
		if totalUsedSize-releasingSize <= sizeLimit {
			break
		}
		info := item.instance.Info()
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
			r.stats.offloading.Add(item.instance.Info())
			r.inFlight[item.instance.BaseFileName] = struct{}{} // Mark as in-flight
		}
	} else {
		r.trimAll(count) // Permanently remove
	}

	return evicted, nil
}

// DrainOverflowOffloading removes oldest fractions from offloading queue when it exceeds size limit.
// Selects fractions that haven't finished offloading yet to minimize data loss.
// Used when offloading queue grows too large due to slow remote storage performance.
func (r *fractionRegistry) DrainOverflowOffloading(sizeLimit uint64) []*sealedProxy {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Fast path: skip processing if within size limits
	if r.stats.offloading.totalSizeOnDisk <= sizeLimit {
		return nil
	}

	var drained []*sealedProxy
	offloading := make([]*sealedProxy, 0, len(r.offloading))

	// Filter fractions, keeping those within limits or with remote references
	for _, item := range r.offloading {
		// Keep items that are within limits or already have remote references
		if r.stats.offloading.totalSizeOnDisk <= sizeLimit || item.remote != nil {
			offloading = append(offloading, item)
			continue
		}
		// Drain items that exceed limits and haven't remote references
		drained = append(drained, item)
		r.stats.offloading.Sub(item.instance.Info())
		delete(r.inFlight, item.instance.BaseFileName)
	}

	r.offloading = offloading
	// Rebuild complete list since we modified the middle of the queue
	r.rebuildAllFractions()

	return drained
}

// EvictRemote removes oldest remote fractions based on retention policy.
// Fractions older than retention period are permanently deleted.
// Returns removed fractions or empty slice if nothing to remove.
func (r *fractionRegistry) EvictRemote(retention time.Duration) []*remoteProxy {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	// Find fractions older than retention period
	for _, item := range r.remotes {
		info := item.instance.Info()
		if time.Since(time.UnixMilli(int64(info.CreationTime))) <= retention {
			break // Stop at first fraction within retention
		}
		r.stats.remotes.Sub(info)
		count++
	}

	evicted := r.remotes[:count]
	r.remotes = r.remotes[count:]
	r.trimAll(count) // Remove from complete list

	return evicted
}

// PromoteToLocal moves fractions from sealing to local queue when sealing completes.
// Maintains strict ordering - younger fractions wait for older ones to seal first.
func (r *fractionRegistry) PromoteToLocal(active *activeProxy, sealed *frac.Sealed) {
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
		r.locals = append(r.locals, &sealedProxy{
			proxy:    item.proxy,
			instance: item.sealed,
		})
		r.stats.locals.Add(item.sealed.Info())
		r.stats.sealing.Sub(item.instance.Info())
	}

	// Remove promoted fractions from sealing queue
	r.sealing = r.sealing[promotedCount:]
}

// PromoteToRemote moves fractions from offloading to remote queue when offloading completes.
// Returns false if the fraction was already drained from the queue.
// Special case: Handles fractions that don't require offloading (remote == nil).
// Maintains strict ordering - younger fractions wait for older ones to offload.
func (r *fractionRegistry) PromoteToRemote(sealed *sealedProxy, remote *frac.Remote) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	sealed.remote = remote

	// Special case 1: sealed can be already removed from queue (see DrainOffloadingOverflow)
	if _, ok := r.inFlight[sealed.instance.BaseFileName]; !ok {
		return false
	}

	// Special case 2: remote == nil means fraction doesn't require offloading
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
		r.remotes = append(r.remotes, &remoteProxy{
			proxy:    item.proxy,
			instance: item.remote,
		})

		r.stats.remotes.Add(item.remote.Info())
		r.stats.offloading.Sub(item.instance.Info())
		delete(r.inFlight, item.instance.BaseFileName)
	}

	// Remove promoted fractions from offloading queue
	r.offloading = r.offloading[promotedCount:]

	return true
}

// removeFromOffloading removes a specific fraction from offloading queue.
// O(n) operation that rebuilds the all fractions list.
func (r *fractionRegistry) removeFromOffloading(sealed *sealedProxy) {
	count := 0
	// Filter out the target fraction
	for _, item := range r.offloading {
		if sealed != item {
			r.offloading[count] = item
			count++
		}
	}
	r.offloading = r.offloading[:count]
	r.stats.offloading.Sub(sealed.instance.Info())
	delete(r.inFlight, sealed.instance.BaseFileName)

	// Rebuild complete list since we modified the middle of the queue
	r.rebuildAllFractions()
}

// rebuildAllFractions reconstructs the all fractions list in correct chronological order.
// Order: remote (oldest) → offloading → local → sealing → active (newest)
// Expensive O(n) operation used when direct list modification is insufficient.
func (r *fractionRegistry) rebuildAllFractions() {
	all := make([]frac.Fraction, 0, len(r.all))

	// Collect fractions in correct chronological order: from oldest (remote) to newest (active)
	for _, remote := range r.remotes {
		all = append(all, remote.proxy)
	}
	for _, offloaded := range r.offloading {
		all = append(all, offloaded.proxy)
	}
	for _, sealed := range r.locals {
		all = append(all, sealed.proxy)
	}
	for _, active := range r.sealing {
		all = append(all, active.proxy)
	}
	all = append(all, r.active.proxy)

	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.all = all
	r.updateOldest()
}

// updateOldest recalculates the creation time of the oldest fraction.
// Called after modifications to the complete fractions list.
func (r *fractionRegistry) updateOldest() {
	if len(r.all) > 0 {
		r.oldest = r.all[0].Info().CreationTime
	} else {
		r.oldest = 0
	}
}
