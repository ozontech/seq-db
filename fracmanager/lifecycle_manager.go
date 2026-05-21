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
// It handles rotation, sealing, offloading, and cleanup operations.
// Created once, it coordinates all fraction state transitions.
type lifecycleManager struct {
	infoCache *fracInfoCache    // fraction metadata cache
	provider  *fractionProvider // provider for fraction operations
	flags     *StateManager     // storage state flags
	registry  *fractionRegistry // fraction state registry
	tasks     *TaskManager      // Background offloading tasks

	sealingWg sync.WaitGroup // todo: get rid after removing SealAll in tests
}

func newLifecycleManager(
	infoCache *fracInfoCache,
	provider *fractionProvider,
	flags *StateManager,
	registry *fractionRegistry,
) *lifecycleManager {
	return &lifecycleManager{
		infoCache: infoCache,
		provider:  provider,
		flags:     flags,
		registry:  registry,
		tasks:     NewTaskManager(),
	}
}

// Maintain performs periodic lifecycle management tasks.
// It coordinates rotation, offloading, cleanup based on configuration.
func (lc *lifecycleManager) Maintain(ctx context.Context, cfg *Config, wg *sync.WaitGroup) {
	lc.registry.suspendIfOverCapacity(cfg.SealingQueueLen, cfg.SuspendThreshold())

	lc.rotate(cfg.FracSize, wg)
	if cfg.OffloadingEnabled {
		lc.offloadLocal(ctx, cfg.TotalSize, cfg.OffloadingRetryDelay, wg)
		if cfg.OffloadingQueueSize > 0 {
			lc.removeOverflowed(cfg.OffloadingQueueSize, wg)
		}
		lc.cleanRemote(cfg.OffloadingRetention, wg)
	} else {
		lc.cleanLocal(cfg.TotalSize, wg)
	}
	lc.updateOldestMetric()
	lc.SyncInfoCache()
}

// SyncInfoCache synchronizes the info cache with disk state.
func (lc *lifecycleManager) SyncInfoCache() {
	if err := lc.infoCache.SyncWithDisk(); err != nil {
		logger.Error("can't sync info-cache", zap.Error(err))
	}
}

// rotate checks if active fraction needs rotation based on size limit.
// Creates new active fraction and starts sealing the previous one.
func (lc *lifecycleManager) rotate(maxSize uint64, wg *sync.WaitGroup) {
	active, waitBeforeSealing, err := lc.registry.rotateIfFull(maxSize, lc.provider)
	if err != nil {
		logger.Fatal("active fraction rotation error", zap.Error(err))
	}
	if active == nil {
		return
	}

	wg.Add(1)
	lc.sealingWg.Add(1)
	go func() {
		defer wg.Done()
		defer lc.sealingWg.Done()

		waitBeforeSealing()
		sealed, err := lc.provider.Seal(active.Active)
		if err != nil {
			logger.Fatal("sealing error", zap.Error(err))
		}

		lc.infoCache.Add(sealed.Info())
		lc.registry.promoteToSealed(active, sealed)
		active.Destroy()
	}()
}

// offloadLocal starts offloading of local fractions to remote storage.
// Selects fractions based on disk space usage and retention policy.
func (lc *lifecycleManager) offloadLocal(ctx context.Context, sizeLimit uint64, retryDelay time.Duration, wg *sync.WaitGroup) {
	toOffload, err := lc.registry.evictLocalForOffload(sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	for _, frac := range toOffload {
		wg.Add(1)
		_, err := lc.tasks.Run(frac.BaseFileName, ctx, func(ctx context.Context) {
			defer wg.Done()

			remote := lc.offloadWithRetry(ctx, frac.Sealed, retryDelay)

			lc.registry.promoteToRemote(frac, remote)

			if remote == nil {
				lc.infoCache.Remove(frac.Info().Name())
			}

			// free up local resources
			frac.Destroy()
			maintenanceTruncateTotal.Add(1)
		})
		if err != nil {
			panic(err) // we do not expect error here
		}
	}
}

// OffloadWithRetry attempts to offload a fraction with retries until success or cancellation.
// Returns the remote fraction instance and a boolean indicating whether offloading was not canceled.
func (lc *lifecycleManager) offloadWithRetry(ctx context.Context, sealed *frac.Sealed, retryDelay time.Duration) *frac.Remote {
	start := time.Now()
	for i := 0; ; i++ {
		remote, err := lc.tryOffload(ctx, sealed)
		if err == nil {
			return remote
		}

		logger.Warn(
			"fail to offload fraction",
			zap.String("name", sealed.BaseFileName),
			zap.Duration("offloading_time", time.Since(start)),
			zap.Int("attempts", i),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			logger.Info(
				"fraction offloading was stopped",
				zap.String("name", sealed.BaseFileName),
				zap.Duration("offloading_time", time.Since(start)),
				zap.Int("attempts", i),
				zap.Error(ctx.Err()),
			)
			return nil
		case <-time.After(retryDelay):
			// Wait before next retry attempt
		}
	}
}

// tryOffload performs a single offload attempt and records metrics.
// Measures offloading duration and tracks success/failure statistics.
func (lc *lifecycleManager) tryOffload(ctx context.Context, sealed *frac.Sealed) (*frac.Remote, error) {
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

// cleanRemote deletes outdated remote fractions based on retention policy.
func (lc *lifecycleManager) cleanRemote(retention time.Duration, wg *sync.WaitGroup) {
	toDelete := lc.registry.evictRemote(retention)
	wg.Add(len(toDelete))
	for _, remote := range toDelete {
		go func() {
			defer wg.Done()
			lc.infoCache.Remove(remote.Info().Name())
			remote.Destroy()
		}()
	}
}

// cleanLocal deletes outdated local fractions when offloading is disabled.
func (lc *lifecycleManager) cleanLocal(sizeLimit uint64, wg *sync.WaitGroup) {
	toDelete, err := lc.registry.evictLocalForDelete(sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}

	if len(toDelete) > 0 && !lc.flags.IsCapacityExceeded() {
		if err := lc.flags.setCapacityExceeded(true); err != nil {
			logger.Fatal("can't set capacity_exceeded flag", zap.Error(err))
		}
	}

	wg.Add(len(toDelete))
	for _, frac := range toDelete {
		go func() {
			defer wg.Done()
			lc.infoCache.Remove(frac.Info().Name())
			frac.Destroy()
			maintenanceTruncateTotal.Add(1)
		}()
	}
}

// updateOldestMetric updates the prometheus metric with oldest fraction timestamp.
func (lc *lifecycleManager) updateOldestMetric() {
	oldestFracTime.WithLabelValues("remote").Set((time.Duration(lc.registry.oldestTotal()) * time.Millisecond).Seconds())
	oldestFracTime.WithLabelValues("local").Set((time.Duration(lc.registry.oldestLocal()) * time.Millisecond).Seconds())
}

// removeOverflowed removes fractions from offloading queue that exceed size limit
// Stops ongoing offloading tasks and cleans up both local and remote resources.
func (lc *lifecycleManager) removeOverflowed(sizeLimit uint64, wg *sync.WaitGroup) {
	evicted := lc.registry.evictOverflowed(sizeLimit)
	for _, sealed := range evicted {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Cancel the offloading task - this operation may take significant time
			// hence executed in a separate goroutine to avoid blocking
			lc.tasks.Cancel(sealed.BaseFileName)
		}()
	}
}
