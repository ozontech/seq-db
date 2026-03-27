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
// The entire structure is thread-safe due to internal synchronization.
type fractionRegistry struct {
	mu sync.RWMutex // main mutex for protecting registry state

	sealing    map[string]*syncAppender                 // fractions being sealed (0-5 typical)
	sealed     PartitionedCollection[*refCountedSealed] // local sealed fractions (can be thousands)
	offloading PartitionedCollection[*refCountedSealed] // fractions being offloaded (0-5 typical)
	remotes    PartitionedCollection[*refCountedRemote] // offloaded fractions (can be thousands)

	stats registryStats // size statistics for monitoring

	muAppender sync.RWMutex
	appender   *syncAppender // currently active writable fraction

	muAll sync.RWMutex
	all   fractionsSnapshot // all fractions
}

// NewFractionRegistry creates and initializes a new fraction registry instance.
// Populates the registry with existing active, sealed and remote fractions.
func NewFractionRegistry(active *frac.Active, sealed []*frac.Sealed, remotes []*frac.Remote) (*fractionRegistry, error) {
	if active == nil {
		return nil, errors.New("active fraction must be specified")
	}

	creationTime := func(f frac.Fraction) uint64 { return f.Info().CreationTime }

	lastDocTime := func(f frac.Fraction) uint64 {
		aligned := f.Info().To.Time().
			Add(-time.Nanosecond).
			Truncate(time.Minute).
			Add(time.Minute)
		return uint64(aligned.UnixMilli())
	}

	reg := fractionRegistry{
		appender: &syncAppender{refCountedActive: refCountedActive{Active: active}},

		sealing:    map[string]*syncAppender{},
		sealed:     NewPartitionedCollection(func(rcs *refCountedSealed) uint64 { return creationTime(rcs) }),
		offloading: NewPartitionedCollection(func(rcs *refCountedSealed) uint64 { return lastDocTime(rcs) }),
		remotes:    NewPartitionedCollection(func(rcr *refCountedRemote) uint64 { return lastDocTime(rcr) }),
	}

	// initialize local sealed fractions
	for _, s := range sealed {
		reg.stats.sealed.Add(s.Info())
		reg.sealed.Add(s.Info().Name(), &refCountedSealed{Sealed: s})
	}

	// initialize remote fractions
	for _, r := range remotes {
		reg.stats.remotes.Add(r.Info())
		reg.remotes.Add(r.Info().Name(), &refCountedRemote{Remote: r})
	}

	reg.rebuildSnapshot()

	return &reg, nil
}

// Appender returns the currently active writable fraction.
func (r *fractionRegistry) Appender() *syncAppender {
	r.muAppender.RLock()
	defer r.muAppender.RUnlock()
	return r.appender
}

func (r *fractionRegistry) AcquireOneFraction(name string) (frac.Fraction, func(), bool) {
	r.muAll.RLock()
	defer r.muAll.RUnlock()

	return r.all.AcquireOne(name)
}

// AcquireAllFractions returns a read-only view of all fractions
func (r *fractionRegistry) AcquireAllFractions() ([]frac.Fraction, func()) {
	r.muAll.RLock()
	defer r.muAll.RUnlock()

	return r.all.AcquireAll()
}

// Stats returns current size statistics of the registry.
func (r *fractionRegistry) Stats() registryStats {
	r.mu.RLock()
	s := r.stats
	i := r.appender.Info()
	r.mu.RUnlock()

	s.active.Set(i)
	return s
}

// OldestTotal returns the creation time of the oldest fraction in the registry.
func (r *fractionRegistry) OldestTotal() uint64 {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.all.oldestTotal
}

// OldestLocal returns the creation time of the oldest local fraction in the registry.
func (r *fractionRegistry) OldestLocal() uint64 {
	r.muAll.RLock()
	defer r.muAll.RUnlock()
	return r.all.oldestLocal
}

type activeProvider interface {
	CreateActive() *frac.Active
}

func (r *fractionRegistry) setAppender(appender *syncAppender) {
	r.muAppender.Lock()
	defer r.muAppender.Unlock()

	r.appender = appender

	r.muAll.Lock()
	defer r.muAll.Unlock()

	r.all.AddActive(appender)
}

// RotateIfFull completes the current active fraction and starts a new one.
// Moves previous active fraction to sealing queue.
// Should be called when the current active fraction reaches size limit and needs to be rotated
func (r *fractionRegistry) RotateIfFull(maxSize uint64, ap activeProvider) (*refCountedActive, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.appender.Info().DocsOnDisk <= maxSize {
		return nil, nil, nil
	}

	old := r.appender

	r.sealing[old.Info().Name()] = old

	r.setAppender(&syncAppender{refCountedActive: refCountedActive{Active: ap.CreateActive()}})

	if err := old.Finalize(); err != nil {
		return nil, nil, err
	}

	curInfo := old.Info()
	r.stats.sealing.Add(curInfo)

	r.appender.Suspend(old.Suspended())

	wg := sync.WaitGroup{}
	wg.Add(1)
	// since old.WaitWriteIdle() can take some time, we don't want to do it under the lock
	// we will do it asynchronously in a goroutine
	go func() {
		defer wg.Done()

		old.WaitWriteIdle() // can be long enough
		finalInfo := old.Info()

		r.mu.Lock()
		defer r.mu.Unlock()

		// curInfo and finalInfo differ because while we are waiting for old.WaitWriteIdle(),
		// the latest data is being written to the active fraction index
		r.stats.sealing.Sub(curInfo)
		r.stats.sealing.Add(finalInfo)
	}()

	return &old.refCountedActive, wg.Wait, nil
}

func (r *fractionRegistry) SuspendIfOverCapacity(maxQueue, maxSize uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	suspended := r.appender.Suspended()

	if maxQueue > 0 && r.stats.sealing.count >= int(maxQueue) {
		if !suspended {
			logger.Warn("switching to read-only mode",
				zap.String("reason", "sealing queue size exceeded"),
				zap.Uint64("limit", maxQueue),
				zap.Int("queue_size", r.stats.sealing.count))
			r.appender.Suspend(true)
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
			r.appender.Suspend(true)
		}
		return
	}

	if suspended {
		logger.Warn("switching to write mode",
			zap.Float64("queue_size_limit_gb", util.Float64ToPrec(util.SizeToUnit(maxSize, "gb"), 2)),
			zap.Float64("occupied_space_gb", util.Float64ToPrec(util.SizeToUnit(du, "gb"), 2)),
			zap.Uint64("sealing_queue_size_limit", maxQueue),
			zap.Int("queue_size", r.stats.sealing.count))
		r.appender.Suspend(false)
	}
}

func (r *fractionRegistry) diskUsage() uint64 {
	return r.appender.Info().FullSize() +
		r.stats.sealed.totalSizeOnDisk +
		r.stats.sealing.totalSizeOnDisk +
		r.stats.offloading.totalSizeOnDisk
}

// EvictLocalForDelete removes oldest local fractions to free disk space.
// Returns evicted fractions or error if insufficient space is released.
func (r *fractionRegistry) EvictLocalForDelete(sizeLimit uint64) (evicted []*refCountedSealed, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if evicted, err = r.evictLocal(sizeLimit); err != nil {
		return nil, err
	}

	r.rebuildSnapshot()

	return evicted, nil
}

// EvictLocalForOffload removes oldest local fractions to moves it to offloading queue.
// Returns evicted fractions or error if insufficient space is released.
func (r *fractionRegistry) EvictLocalForOffload(sizeLimit uint64) ([]*refCountedSealed, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	evicted, err := r.evictLocal(sizeLimit)
	if err != nil {
		return nil, err
	}

	for _, sealed := range evicted {
		r.offloading.Add(sealed.Info().Name(), sealed)
		r.stats.offloading.Add(sealed.Info())
	}

	return evicted, nil
}

func (r *fractionRegistry) evictLocal(sizeLimit uint64) ([]*refCountedSealed, error) {
	var releasingSize uint64

	// calculate total used disk space
	totalUsedSize := r.stats.TotalSizeOnDiskLocal() + r.appender.Info().FullSize()

	evicted := []*refCountedSealed{}

	for r.sealed.Len() > 0 && totalUsedSize-releasingSize > sizeLimit {
		for _, s := range r.sealed.GetByPartition(r.sealed.MinPartition()) {
			info := s.Info()
			releasingSize += info.FullSize()
			r.stats.sealed.Sub(info)
			r.sealed.Del(info.Name())
			evicted = append(evicted, s)
		}
	}

	// check if enough space will be freed
	if totalUsedSize-releasingSize > sizeLimit {
		return nil, fmt.Errorf("insufficient space released: need to free %d more bytes "+
			"(total: %d, releasing: %d, limit: %d)",
			(totalUsedSize-releasingSize)-sizeLimit, totalUsedSize, releasingSize, sizeLimit)
	}

	return evicted, nil
}

// EvictRemote removes oldest remote fractions based on retention policy.
// Fractions older than retention period are permanently deleted.
// Returns removed fractions or empty slice if nothing to remove.
func (r *fractionRegistry) EvictRemote(retention time.Duration) []*refCountedRemote {
	if retention == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	evicted := []*refCountedRemote{}
	for r.remotes.Len() > 0 && time.Since(time.UnixMilli(int64(r.remotes.MinPartition()))) > retention {
		for _, remote := range r.remotes.GetByPartition(r.remotes.MinPartition()) {
			info := remote.Info()
			r.stats.remotes.Sub(info)
			evicted = append(evicted, remote)
			r.remotes.Del(info.Name())
		}
	}

	r.rebuildSnapshot()

	return evicted
}

// EvictOverflowed removes oldest fractions from offloading queue when it exceeds size limit.
// Used when offloading queue grows too large due to slow remote storage performance.
func (r *fractionRegistry) EvictOverflowed(sizeLimit uint64) (evicted []*refCountedSealed) {
	if sizeLimit == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Fast path: skip processing if within size limits
	if r.stats.offloading.totalSizeOnDisk <= sizeLimit {
		return nil
	}

loop:
	// filter fractions
	for r.offloading.Len() > 0 {
		for _, s := range r.offloading.GetByPartition(r.offloading.MinPartition()) {
			evicted = append(evicted, s)
			r.stats.offloading.Sub(s.Info())
			r.offloading.Del(s.Info().Name())
			if r.stats.offloading.totalSizeOnDisk <= sizeLimit {
				break loop
			}
		}
	}

	r.rebuildSnapshot()

	return evicted
}

// PromoteToSealed moves fractions from sealing to local queue when sealing completes.
func (r *fractionRegistry) PromoteToSealed(active *refCountedActive, sealed *frac.Sealed) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sealed.Add(sealed.Info().Name(), &refCountedSealed{Sealed: sealed})
	r.stats.sealed.Add(sealed.Info())
	r.stats.sealing.Sub(active.Info())

	delete(r.sealing, active.Info().Name())

	r.rebuildSnapshot()
}

// PromoteToRemote moves fractions from offloading to remote queue when offloading completes.
// Special case: handles fractions that don't require offloading (remote == nil).
func (r *fractionRegistry) PromoteToRemote(sealed *refCountedSealed, remote *frac.Remote) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if remote != nil {
		r.remotes.Add(remote.Info().Name(), &refCountedRemote{Remote: remote})
		r.stats.remotes.Add(remote.Info())
	}

	r.stats.offloading.Sub(sealed.Info())
	r.offloading.Del(sealed.Info().Name())
	r.rebuildSnapshot()
}

// rebuildSnapshot reconstructs the all fractions list
func (r *fractionRegistry) rebuildSnapshot() {
	capacity := r.remotes.Len() + r.offloading.Len() + r.sealed.Len() + len(r.sealing) + 1

	// allocate extra capacity to accommodate appender rotation that may occur during snapshot lifetime
	all := newFractionsSnapshot(capacity + 1)

	for r := range r.remotes.All() {
		all.AddRemote(r)
	}

	for o := range r.offloading.All() {
		all.AddSealed(o)
	}

	for s := range r.sealed.All() {
		all.AddSealed(s)
	}

	for _, a := range r.sealing {
		all.AddActive(a)
	}

	all.AddActive(r.appender)

	r.muAll.Lock()
	defer r.muAll.Unlock()
	r.all = all
}
