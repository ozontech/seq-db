package fracmanager

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
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

	sealingWg sync.WaitGroup
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
	suspendThreshold := cfg.TotalSize + cfg.TotalSize/100 + cfg.OffloadingQueueSize
	lc.registry.SuspendIfOverCapacity(cfg.SealingQueueLen, suspendThreshold)

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

// seal converts an active fraction to sealed state.
// It freezes writes, waits for pending operations, then seals the fraction.
func (lc *lifecycleManager) seal(active *activeProxy) error {
	sealsTotal.Inc()
	now := time.Now()
	sealed, err := lc.provider.Seal(active.instance)
	if err != nil {
		return err
	}
	sealingTime := time.Since(now)
	sealsDoneSeconds.Observe(sealingTime.Seconds())

	logger.Info(
		"fraction sealed",
		zap.String("fraction", filepath.Base(sealed.BaseFileName)),
		zap.Float64("time_spent_s", util.DurationToUnit(sealingTime, "s")),
	)

	lc.infoCache.Add(sealed.Info())
	lc.registry.PromoteToSealed(active, sealed)
	active.proxy.Redirect(sealed)
	active.instance.Release()
	return nil
}

// rotate checks if active fraction needs rotation based on size limit.
// Creates new active fraction and starts sealing the previous one.
func (lc *lifecycleManager) rotate(maxSize uint64, wg *sync.WaitGroup) {
	activeToSeal, waitBeforeSealing, err := lc.registry.RotateIfFull(maxSize, func() *activeProxy {
		return newActiveProxy(lc.provider.CreateActive())
	})
	if err != nil {
		logger.Fatal("active fraction rotation error", zap.Error(err))
	}
	if activeToSeal == nil {
		return
	}

	wg.Add(1)
	lc.sealingWg.Add(1)
	go func() {
		defer wg.Done()
		defer lc.sealingWg.Done()

		waitBeforeSealing()
		if err := lc.seal(activeToSeal); err != nil {
			logger.Fatal("sealing error", zap.Error(err))
		}
	}()
}

// offloadLocal starts offloading of local fractions to remote storage.
// Selects fractions based on disk space usage and retention policy.
func (lc *lifecycleManager) offloadLocal(ctx context.Context, sizeLimit uint64, retryDelay time.Duration, wg *sync.WaitGroup) {
	toOffload, err := lc.registry.EvictLocal(true, sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	for _, sealed := range toOffload {
		wg.Add(1)
		_, err := lc.tasks.Run(sealed.instance.BaseFileName, ctx, func(ctx context.Context) {
			defer wg.Done()

			remote := lc.offloadWithRetry(ctx, sealed.instance, retryDelay)

			lc.registry.PromoteToRemote(sealed, remote)

			if remote == nil {
				sealed.proxy.Redirect(emptyFraction{})
				lc.infoCache.Remove(sealed.instance.Info().Name())
			} else {
				sealed.proxy.Redirect(remote)
			}

			// free up local resources
			sealed.instance.Suicide()
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

// cleanLocal deletes outdated local fractions when offloading is disabled.
func (lc *lifecycleManager) cleanLocal(sizeLimit uint64, wg *sync.WaitGroup) {
	toDelete, err := lc.registry.EvictLocal(false, sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	if len(toDelete) > 0 && !lc.flags.IsCapacityExceeded() {
		if err := lc.flags.setCapacityExceeded(true); err != nil {
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

// updateOldestMetric updates the prometheus metric with oldest fraction timestamp.
func (lc *lifecycleManager) updateOldestMetric() {
	oldestFracTime.WithLabelValues("remote").Set((time.Duration(lc.registry.OldestTotal()) * time.Millisecond).Seconds())
	oldestFracTime.WithLabelValues("local").Set((time.Duration(lc.registry.OldestLocal()) * time.Millisecond).Seconds())
}

// removeOverflowed removes fractions from offloading queue that exceed size limit
// Stops ongoing offloading tasks and cleans up both local and remote resources.
func (lc *lifecycleManager) removeOverflowed(sizeLimit uint64, wg *sync.WaitGroup) {
	evicted := lc.registry.EvictOverflowed(sizeLimit)
	for _, item := range evicted {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Cancel the offloading task - this operation may take significant time
			// hence executed in a separate goroutine to avoid blocking
			lc.tasks.Cancel(item.instance.BaseFileName)
		}()
	}
}
