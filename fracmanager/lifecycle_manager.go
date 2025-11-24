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
	infoCache *fracInfoCache    // Fraction metadata cache
	provider  *fractionProvider // Provider for fraction operations
	flags     *StateManager     // Storage state flags
	registry  *fractionRegistry // Fraction state registry

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
	}
}

// Maintain performs periodic lifecycle management tasks.
// It is a CORE method of lifecycleManager
// Coordinates rotation, offloading, cleanup based on configuration.
func (lc *lifecycleManager) Maintain(ctx context.Context, config *Config, wg *sync.WaitGroup) {
	lc.Rotate(config.FracSize, wg)
	if config.OffloadingEnabled {
		lc.OffloadLocal(ctx, config.TotalSize, wg)
		lc.CleanRemote(config.OffloadingRetention, wg)
	} else {
		lc.CleanLocal(config.TotalSize, wg)
	}
	lc.UpdateOldestMetric()
	lc.SyncInfoCache()
}

func (lc *lifecycleManager) SyncInfoCache() {
	if err := lc.infoCache.SyncWithDisk(); err != nil {
		logger.Error("can't sync info-cache", zap.Error(err))
	}
}

// Rotate checks if active fraction needs rotation based on size limit
// Creates new active fraction and starts sealing the previous one.
func (lc *lifecycleManager) Rotate(maxSize uint64, wg *sync.WaitGroup) {
	activeToSeal, waitBeforeSealing, err := lc.registry.RotateIfFull(maxSize, lc.provider.CreateActive)
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

		sealed, err := lc.provider.Seal(activeToSeal.active)
		if err != nil {
			logger.Fatal("sealing error", zap.Error(err))
		}

		lc.infoCache.Add(sealed.Info())
		lc.registry.PromoteToLocal(activeToSeal, sealed)
		activeToSeal.Destroy()
	}()
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
		go func() {
			defer wg.Done()

			remote, _ := lc.TryOffload(ctx, sealed.sealed)
			lc.registry.PromoteToRemote(sealed, remote)

			if remote == nil {
				lc.infoCache.Remove(sealed.sealed.Info().Name())
			}

			// Free up local resources
			sealed.Destroy()
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
func (lc *lifecycleManager) CleanRemote(retention time.Duration, wg *sync.WaitGroup) {
	if retention == 0 {
		return
	}
	toDelete := lc.registry.EvictRemote(retention)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, remote := range toDelete {
			lc.infoCache.Remove(remote.remote.Info().Name())
			remote.Destroy()
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
		if err := lc.flags.setCapacityExceeded(true); err != nil {
			logger.Fatal("can't set capacity_exceeded flag", zap.Error(err))
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, sealed := range toDelete {
			lc.infoCache.Remove(sealed.sealed.Info().Name())
			sealed.Destroy()
			maintenanceTruncateTotal.Add(1)
		}
	}()
}

// UpdateOldestMetric updates the prometheus metric with oldest fraction timestamp
func (lc *lifecycleManager) UpdateOldestMetric() {
	oldestFracTime.WithLabelValues("remote").Set((time.Duration(lc.registry.OldestTotal()) * time.Millisecond).Seconds())
	oldestFracTime.WithLabelValues("local").Set((time.Duration(lc.registry.OldestLocal()) * time.Millisecond).Seconds())
}
