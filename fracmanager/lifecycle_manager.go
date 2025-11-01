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
// Handles rotation, sealing, offloading, and cleanup operations.
// Lifecycle: Created once, coordinates all fraction state transitions.
type lifecycleManager struct {
	infoCache *fracInfoCache    // Fraction metadata cache
	provider  *fractionProvider // Provider for fraction operations
	flags     *StateManager     // Storage state flags
	registry  *fractionRegistry // Fraction state registry

	wg struct {
		sealing    sync.WaitGroup
		offloading sync.WaitGroup
		cleanup    sync.WaitGroup
	}
}

func newLifecycleManager(
	config *Config,
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
	}
}

// Maintain performs periodic lifecycle management tasks.
// It is a CORE method of lifecycleManager
// Coordinates rotation, offloading, cleanup based on configuration.
func (lc *lifecycleManager) Maintain(ctx context.Context, config *Config) {
	lc.Rotate(config.FracSize)
	if config.OffloadingEnabled {
		lc.OffloadLocal(ctx, config.TotalSize)
		lc.CleanRemote(config.OffloadingRetention)
	} else {
		lc.CleanLocal(config.TotalSize)
	}
	lc.UpdateOldestMetric()
	lc.SyncInfoCache()
}

func (lc *lifecycleManager) WaitMaintain() {
	lc.wg.sealing.Wait()
	lc.wg.offloading.Wait()
	lc.wg.cleanup.Wait()
}

func (lc *lifecycleManager) WaitSealing() {
	lc.wg.sealing.Wait()
}

func (lc *lifecycleManager) WaitOffloading() {
	lc.wg.offloading.Wait()
}

func (lc *lifecycleManager) WaitCleanup() {
	lc.wg.cleanup.Wait()
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
	sealingTime := time.Since(now)
	sealsDoneSeconds.Observe(sealingTime.Seconds())

	logger.Info(
		"fraction sealed",
		zap.String("fraction", filepath.Base(sealed.BaseFileName)),
		zap.Float64("time_spent_s", util.DurationToUnit(sealingTime, "s")),
	)

	lc.infoCache.Add(sealed.Info())
	lc.registry.PromoteToLocal(active, sealed)
	active.proxy.Redirect(sealed)
	active.instance.Release()
	return nil
}

// RotateIfNeeded checks if active fraction needs rotation based on size limit
// Creates new active fraction and starts sealing the previous one.
func (lc *lifecycleManager) Rotate(sizeLimit uint64) {
	if lc.registry.Active().instance.Info().DocsOnDisk > sizeLimit {
		active := lc.rotate()

		lc.wg.sealing.Add(1)
		go func() {
			defer lc.wg.sealing.Done()
			if err := lc.Seal(active); err != nil {
				logger.Fatal("sealing error", zap.Error(err))
			}
		}()
	}
}

func (lc *lifecycleManager) rotate() *activeProxy {
	active, err := lc.registry.Rotate(newActiveProxy(lc.provider.CreateActive()))
	if err != nil {
		logger.Fatal("active fraction rotation error", zap.Error(err))
	}
	return active
}

// OffloadLocal starts offloading of local fractions to remote storage
// Selects fractions based on disk space usage and retention policy.
func (lc *lifecycleManager) OffloadLocal(ctx context.Context, sizeLimit uint64) {
	toOffload, err := lc.registry.EvictLocal(true, sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	for _, sealed := range toOffload {
		lc.wg.offloading.Add(1)
		go func() {
			defer lc.wg.offloading.Done()

			remote, _ := lc.TryOffload(ctx, sealed.instance)
			lc.registry.PromoteToRemote(sealed, remote)

			if remote == nil {
				sealed.proxy.Redirect(emptyFraction{})
			} else {
				sealed.proxy.Redirect(remote)
			}

			// Free up local resources
			sealed.instance.Suicide()
			maintenanceTruncateTotal.Add(1)
		}()
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

// CleanRemote deletes outdated remote fractions based on retention policy
func (lc *lifecycleManager) CleanRemote(retention time.Duration) {
	if retention == 0 {
		return
	}
	toDelete := lc.registry.EvictRemote(retention)
	lc.wg.cleanup.Add(1)
	go func() {
		defer lc.wg.cleanup.Done()
		for _, remote := range toDelete {
			remote.proxy.Redirect(emptyFraction{})
			lc.infoCache.Remove(remote.instance.Info().Name())
			remote.instance.Suicide()
		}
	}()
}

// CleanLocal deletes outdated local fractions when offloading is disabled
func (lc *lifecycleManager) CleanLocal(sizeLimit uint64) {
	toDelete, err := lc.registry.EvictLocal(false, sizeLimit)
	if err != nil {
		logger.Fatal("error releasing old fractions:", zap.Error(err))
	}
	if len(toDelete) > 0 && !lc.flags.IsCapacityExceeded() {
		if err := lc.flags.setCapacityExceeded(true); err != nil {
			logger.Fatal("can't set capacity_exceeded flag", zap.Error(err))
		}
	}

	lc.wg.cleanup.Add(1)
	go func() {
		defer lc.wg.cleanup.Done()
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
	oldestFracTime.WithLabelValues("remote").Set((time.Duration(lc.registry.OldestTotal()) * time.Millisecond).Seconds())
	oldestFracTime.WithLabelValues("local").Set((time.Duration(lc.registry.OldestLocal()) * time.Millisecond).Seconds())
}
