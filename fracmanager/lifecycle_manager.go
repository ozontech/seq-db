package fracmanager

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
)

// lifecycleManager manages the complete lifecycle of fractions.
// Handles rotation, sealing, offloading, and cleanup operations.
// Lifecycle: Created once, coordinates all fraction state transitions.
type lifecycleManager struct {
	config    *Config           // Configuration with fraction management parameters
	infoCache *fracInfoCache    // Fraction metadata cache
	provider  *fractionProvider // Provider for fraction operations
	flags     *StateManager     // Storage state flags
	registry  *fractionRegistry // Fraction state registry
	tasks     *TaskManager      // Background offloading tasks
}

func newLifecycleManager(
	config *Config,
	infoCache *fracInfoCache,
	provider *fractionProvider,
	flags *StateManager,
	registry *fractionRegistry,
) *lifecycleManager {
	return &lifecycleManager{
		config:    config,
		infoCache: infoCache,
		provider:  provider,
		flags:     flags,
		registry:  registry,
		tasks:     NewTaskManager(),
	}
}

// Maintain performs periodic lifecycle management tasks.
// It is a CORE method of lifecycleManager
// Coordinates rotation, offloading, cleanup based on configuration.
func (lc *lifecycleManager) Maintain(ctx context.Context, wg *sync.WaitGroup) {
	lc.RotateIfNeeded(lc.config.FracSize, wg)
	if lc.config.OffloadingEnabled {
		lc.OffloadLocal(ctx, lc.config.TotalSize, wg)
		lc.DrainExcessOffloads(lc.config.OffloadingSize, wg)
		lc.CleanRemote(lc.config.OffloadingRetention, wg)
	} else {
		lc.CleanLocal(lc.config.TotalSize, wg)
	}
	lc.UpdateOldestMetric()
	lc.SyncInfoCache()
}

func (lc *lifecycleManager) SyncInfoCache() {
	if err := lc.infoCache.SyncWithDisk(); err != nil {
		logger.Error("can't sync info-cache", zap.Error(err))
	}
}

// Seal converts an active fraction to sealed state
// Freezes writes, waits for pending operations, then seals the fraction.
func (lc *lifecycleManager) Seal(active *activeProxy) error {
	now := time.Now()
	sealed, err := lc.provider.Seal(active.instance)
	if err != nil {
		return err
	}
	sealsTotal.Inc()
	sealsDoneSeconds.Observe(time.Since(now).Seconds())

	lc.infoCache.Add(sealed.Info())
	lc.registry.PromoteToLocal(active, sealed)
	active.proxy.Redirect(sealed)
	active.instance.Release()
	return nil
}

// RotateIfNeeded checks if active fraction needs rotation based on size limit
// Creates new active fraction and starts sealing the previous one.
func (lc *lifecycleManager) RotateIfNeeded(sizeLimit uint64, wg *sync.WaitGroup) {
	if lc.registry.Active().instance.Info().DocsOnDisk > sizeLimit {
		active := lc.Rotate()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lc.Seal(active); err != nil {
				logger.Fatal("sealing error", zap.Error(err))
			}
		}()
	}
}

func (lc *lifecycleManager) Rotate() *activeProxy {
	active, err := lc.registry.Rotate(newActiveProxy(lc.provider.CreateActive()))
	if err != nil {
		logger.Fatal("active fraction rotation error", zap.Error(err))
	}
	return active
}

// OffloadLocal starts offloading of local fractions to remote storage
// Selects fractions based on disk space usage and retention policy.
func (lc *lifecycleManager) OffloadLocal(ctx context.Context, sizeLimit uint64, wg *sync.WaitGroup) {
	toOffload, err := lc.registry.EvictLocal(true, sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	for _, sealed := range toOffload {
		wg.Add(1)
		lc.tasks.Run(sealed.instance.BaseFileName, ctx, func(ctx context.Context) {
			defer wg.Done()
			if remote, completed := lc.OffloadWithRetry(sealed, ctx); completed {
				// Complete offloading only if it wasn't canceled
				lc.CompleteOffload(sealed, remote)
			}
		})
	}
}

// OffloadWithRetry attempts to offload a fraction with retries until success or cancellation.
// Uses a fixed retry delay configured by OffloadRetryDelay.
// Returns the remote fraction instance and a boolean indicating whether offloading was not canceled.
func (lc *lifecycleManager) OffloadWithRetry(sealed *sealedProxy, ctx context.Context) (*frac.Remote, bool) {
	start := time.Now()
	for i := 0; ; i++ {
		remote, err := lc.TryOffload(ctx, sealed.instance)
		if err == nil {
			return remote, true
		}

		logger.Warn(
			"fail to offload fraction",
			zap.String("name", sealed.instance.BaseFileName),
			zap.Duration("offloading_time", time.Since(start)),
			zap.Int("attempts", i),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			logger.Info(
				"fraction offloading was stopped",
				zap.String("name", sealed.instance.BaseFileName),
				zap.Duration("offloading_time", time.Since(start)),
				zap.Int("attempts", i),
				zap.Error(ctx.Err()),
			)
			return nil, false
		case <-time.After(lc.config.OffloadRetryDelay):
			// Wait before next retry attempt
		}
	}
}

// TryOffload performs a single offload attempt and records metrics
// Measures offloading duration and tracks success/failure statistics.
func (lc *lifecycleManager) TryOffload(ctx context.Context, sealed *frac.Sealed) (*frac.Remote, error) {
	now := time.Now()
	remote, err := lc.provider.Offload(ctx, sealed)
	offloadingDuration := time.Since(now).Seconds()

	if err != nil {
		offloadingTotal.WithLabelValues("failure").Inc()
		offloadingDurationSeconds.Observe(float64(offloadingDuration))
		return nil, err
	}

	if remote != nil {
		offloadingTotal.WithLabelValues("success").Inc()
		offloadingDurationSeconds.Observe(float64(offloadingDuration))
	}

	return remote, nil
}

// CompleteOffload finalizes the offloading process for a successfully offloaded fraction:
// Updates registry state, redirects proxy references, and releases local resources.
// No action is taken if the fraction was already drained from the registry.
// Drained fractions are handled in DrainExcessOffloads.
func (lc *lifecycleManager) CompleteOffload(sealed *sealedProxy, remote *frac.Remote) {
	if !lc.registry.PromoteToRemote(sealed, remote) {
		// can't promote - it is drained
		return
	}

	if remote == nil {
		sealed.proxy.Redirect(emptyFraction{})
	} else {
		sealed.proxy.Redirect(remote)
	}

	// Free up local resources
	sealed.instance.Suicide()
	maintenanceTruncateTotal.Add(1)
}

// DrainExcessOffloads removes fractions from offloading queue that exceed size limit
// Stops ongoing offloading tasks and cleans up both local and remote resources.
func (lc *lifecycleManager) DrainExcessOffloads(sizeLimit uint64, wg *sync.WaitGroup) {
	drained := lc.registry.DrainOverflowOffloading(sizeLimit)
	for _, item := range drained {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Cancel the offloading task - this operation may take significant time
			// hence executed in a separate goroutine to avoid blocking
			lc.tasks.Cancel(item.instance.BaseFileName)

			// Redirect proxy to empty fraction and clean up local resources
			item.proxy.Redirect(emptyFraction{})
			lc.infoCache.Remove(item.instance.Info().Name())
			item.instance.Suicide()
			maintenanceTruncateTotal.Add(1)

			// At this point, the offloading goroutine is guaranteed to be canceled
			// and the fraction is excluded from the registry, ensuring no asynchronous
			// access to the item.remote field occurs concurrently
			if item.remote != nil {
				// Important: Handles case where local fraction was drained simultaneously with successful offloading.
				// We clean up remote storage data since the fraction was excluded from search.
				// If store crashes at this moment, the fraction will return to search as offloaded on restart.
				// We consider this case as acceptable.
				item.remote.Suicide()
			}
		}()
	}
}

// CleanRemote deletes outdated remote fractions based on retention policy
func (lc *lifecycleManager) CleanRemote(retention time.Duration, wg *sync.WaitGroup) {
	toDelete := lc.registry.EvictRemote(retention)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, remote := range toDelete {
			remote.proxy.Redirect(emptyFraction{})
			lc.infoCache.Remove(remote.instance.Info().Name())
			remote.instance.Suicide()
		}
	}()
}

// CleanLocal deletes outdated local fractions when offloading is disabled
func (lc *lifecycleManager) CleanLocal(sizeLimit uint64, wg *sync.WaitGroup) {
	toDelete, err := lc.registry.EvictLocal(false, sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	if len(toDelete) > 0 && !lc.flags.IsCapacityExceeded() {
		if err := lc.flags.SetCapacityExceeded(true); err != nil {
			logger.Fatal("can't set capacity_exceeded flag", zap.Error(err))
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, sealed := range toDelete {
			sealed.proxy.Redirect(emptyFraction{})
			lc.infoCache.Remove(sealed.instance.Info().Name())
			sealed.instance.Suicide()
			maintenanceTruncateTotal.Add(1)
		}
	}()
}

// UpdateOldestMetric updates the prometheus metric with oldest fraction timestamp
func (lc *lifecycleManager) UpdateOldestMetric() {
	oldest := lc.registry.Oldest()
	if oldest == 0 {
		oldest = uint64(time.Now().Unix())
	}
	oldestFracTime.Set((time.Duration(oldest) * time.Millisecond).Seconds())
}
