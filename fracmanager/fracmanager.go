package fracmanager

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
	"github.com/ozontech/seq-db/util"
)

// FracManager manages database fractions with lifecycle operations
type FracManager struct {
	mu sync.Mutex // todo: get rid of mutex after removing SealForcedForTests method
	lc *lifecycleManager
}

var defaultStorageState = StorageState{
	CapacityExceeded: false,
}

// New creates and initializes a new fraction manager
// Starts all background workers:
//   - indexer,
//   - cache cleaner,
//   - fraction rotation
//   - stats updating
//
// Returns the manager instance and a stop function to gracefully shutdown
func New(ctx context.Context, cfg *Config, s3cli *s3.Client) (*FracManager, func(), error) {
	FillConfigWithDefault(cfg)

	readLimiter := storage.NewReadLimiter(config.ReaderWorkers, storeBytesRead)
	idx, stopIdx := frac.NewActiveIndexer(config.IndexWorkers, config.IndexWorkers)
	cache := NewCacheMaintainer(cfg.CacheSize, cfg.SortCacheSize, newDefaultCacheMetrics())
	provider := newFractionProvider(cfg, s3cli, cache, readLimiter, idx)
	infoCache := NewFracInfoCache(filepath.Join(cfg.DataDir, consts.FracCacheFileSuffix))

	// Load existing fractions into registry
	loader := NewLoader(cfg, provider, infoCache)
	registry, err := loader.Load(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Initialize storage state manager to track capacity status
	storageState, err := NewStateManager(cfg.DataDir, defaultStorageState)
	if err != nil {
		return nil, nil, err
	}

	// Create lc manager to handle fraction maintenance
	lc := newLifecycleManager(infoCache, provider, storageState, registry)
	fm := FracManager{lc: lc}

	// Start background workers and get stop function
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)

	startStatsWorker(ctx, registry, &wg)
	startMaintWorker(ctx, cfg, &fm, &wg)
	startCacheWorker(ctx, cfg, cache, &wg)

	stop := func() {
		n := time.Now()
		logger.Info("start stopping fracmanager's workers")

		cancel()
		wg.Wait()

		// Freeze active fraction to prevent new writes
		active := lc.registry.Active()
		if err := active.Finalize(); err != nil {
			logger.Fatal("shutdown fraction freezing error", zap.Error(err))
		}
		active.WaitWriteIdle()

		// Stop indexer
		stopIdx()

		// Save info cache
		lc.SyncInfoCache()

		// Seal active fraction
		sealOnShutdown(active.instance, provider, cfg.MinSealFracSize)

		logger.Info("fracmanager's workers are stopped", zap.Int64("took_ms", time.Since(n).Milliseconds()))
	}

	return &fm, stop, nil
}

func (fm *FracManager) FractionsSnapshot() (List, ReleaseSnapshot) {
	return fm.lc.registry.FractionsSnapshot()
}

func (fm *FracManager) Oldest() uint64 {
	return fm.lc.registry.OldestTotal()
}

func (fm *FracManager) Flags() *StateManager {
	return fm.lc.flags
}

// Active returns the currently active fraction
func (fm *FracManager) Active() frac.Fraction {
	return fm.lc.registry.Active().instance
}

// Append writes documents and metadata to the active fraction
// Implements retry logic in case of fraction sealing during write
func (fm *FracManager) Append(ctx context.Context, docs, metas storage.DocBlock) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Try to append data to the currently active fraction
			err := fm.lc.registry.Active().Append(docs, metas)
			if err != nil {
				logger.Info("append fail", zap.Error(err))
				if err == ErrFractionNotWritable {
					continue // fraction is currently being sealed, retry the operation
				}
			}
			return err
		}
	}
}

// startCacheWorker starts background cache garbage collection
func startCacheWorker(ctx context.Context, cfg *Config, cache *CacheMaintainer, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		logger.Info("cache cleanup loop is started")
		// Run cache cleanup with specified intervals
		cache.RunCleanLoop(ctx.Done(), cfg.CacheCleanupDelay, cfg.CacheGCDelay)
		logger.Info("cache cleanup loop is stopped")
	}()
}

// startStatsWorker starts periodic statistics collection and reporting
func startStatsWorker(ctx context.Context, reg *fractionRegistry, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		logger.Info("stats loop is started")
		// Run stats collection every 10 seconds
		util.RunEvery(ctx.Done(), time.Second*10, func() {
			stats := reg.Stats()
			stats.Log()        // Log statistics
			stats.SetMetrics() // Update Prometheus metrics
		})
		logger.Info("stats loop is stopped")
	}()
}

// startMaintWorker starts periodic fraction maintenance operations
func startMaintWorker(ctx context.Context, cfg *Config, fm *FracManager, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		logger.Info("maintenance loop is started")
		// Run maintenance at configured interval
		util.RunEvery(ctx.Done(), cfg.MaintenanceDelay, func() {
			n := time.Now()
			logger.Debug("maintenance iteration started")
			fm.mu.Lock()
			// Perform fraction maintenance (rotation, truncating, offloading, etc.)
			fm.lc.Maintain(ctx, cfg, wg)
			fm.mu.Unlock()
			logger.Debug("maintenance iteration finished", zap.Int64("took_ms", time.Since(n).Milliseconds()))
		})
		logger.Info("waiting maintenance complete background tasks")
		logger.Info("maintenance loop is stopped")
	}()
}

// SealOnShutdown seals the active fraction on storage shutdown
func sealOnShutdown(active *frac.Active, provider *fractionProvider, minSealSize uint64) {
	fracSize := active.Info().FullSize()

	if minSealSize == 0 {
		logger.Info("sealing skipped: sealing on shutdown is disabled",
			zap.String("frac", active.BaseFileName),
			zap.Uint64("size_mb", uint64(util.SizeToUnit(fracSize, "mb"))),
		)
		return
	}

	if fracSize < minSealSize {
		logger.Info("sealing skipped: fraction too small",
			zap.String("frac", active.BaseFileName),
			zap.Uint64("size_mb", uint64(util.SizeToUnit(fracSize, "mb"))),
		)
		return
	}

	logger.Info("fraction sealed before shutdown",
		zap.String("frac", active.BaseFileName),
		zap.Uint64("fill_size_mb", uint64(util.SizeToUnit(fracSize, "mb"))),
	)

	if _, err := provider.Seal(active); err != nil {
		logger.Error("error sealing on shutdown", zap.Error(err))
	}
}
