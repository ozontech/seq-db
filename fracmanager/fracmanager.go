package fracmanager

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
	"github.com/ozontech/seq-db/util"
)

type FracManager struct {
	ctx    context.Context
	config *Config

	cacheMaintainer *CacheMaintainer

	fracCache *fracInfoCache

	fracMu      sync.RWMutex
	localFracs  []*fracRef
	remoteFracs []*frac.Remote
	active      activeRef

	indexer      *frac.ActiveIndexer
	fracProvider *fractionProvider

	oldestCTLocal  atomic.Uint64
	oldestCTRemote atomic.Uint64

	flags *StateManager

	stopFn  func()
	statWG  sync.WaitGroup
	mntcWG  sync.WaitGroup
	cacheWG sync.WaitGroup

	s3cli *s3.Client
}

type fracRef struct {
	instance frac.Fraction
}

type activeRef struct {
	ref  *fracRef // ref contains a back reference to the fraction in the slice
	frac *proxyFrac
}

func (fm *FracManager) newActiveRef(active *frac.Active) activeRef {
	f := newProxyFrac(active, fm.fracProvider)
	return activeRef{
		frac: f,
		ref:  &fracRef{instance: f},
	}
}

func New(ctx context.Context, cfg *Config, s3cli *s3.Client) (*FracManager, error) {
	FillConfigWithDefault(cfg)

	cacheMaintainer := NewCacheMaintainer(cfg.CacheSize, cfg.SortCacheSize, newDefaultCacheMetrics())

	readLimiter := storage.NewReadLimiter(config.ReaderWorkers, storeBytesRead)
	indexer := frac.NewActiveIndexer(config.IndexWorkers, config.IndexWorkers)
	indexer.Start()

	flags, err := NewStateManager(cfg.DataDir, StorageState{})
	if err != nil {
		logger.Fatal("state manager initiation error", zap.Error(err))
	}

	fracManager := &FracManager{
		config:          cfg,
		ctx:             ctx,
		s3cli:           s3cli,
		flags:           flags,
		cacheMaintainer: cacheMaintainer,
		indexer:         indexer,
		fracProvider:    newFractionProvider(cfg, s3cli, cacheMaintainer, readLimiter, indexer),
		fracCache:       NewFracInfoCache(filepath.Join(cfg.DataDir, consts.FracCacheFileSuffix)),
	}

	err = fracManager.load(ctx)
	return fracManager, err
}

func (fm *FracManager) maintenance(sealWg, cleanupWg *sync.WaitGroup) {
	logger.Debug("maintenance started")

	n := time.Now()
	if fm.Active().Info().DocsOnDisk > fm.config.FracSize {
		active := fm.rotate()

		sealWg.Add(1)
		go func() {
			fm.seal(active)
			sealWg.Done()
		}()
	}

	fm.cleanupFractions(cleanupWg)
	fm.removeStaleFractions(cleanupWg, fm.config.OffloadingRetention)
	fm.updateOldestCT()

	if err := fm.fracCache.SyncWithDisk(); err != nil {
		logger.Error("can't sync frac-cache", zap.Error(err))
	}

	logger.Debug("maintenance finished", zap.Int64("took_ms", time.Since(n).Milliseconds()))
}

func (fm *FracManager) Oldest() uint64 {
	local, remote := fm.oldestCTLocal.Load(), fm.oldestCTRemote.Load()
	if local != 0 && remote != 0 {
		return min(local, remote)
	}
	return local
}

func (fm *FracManager) updateOldestCT() {
	fm.updateOldestCTFor(fm.getLocalFracs(), &fm.oldestCTLocal, "local")
	fm.updateOldestCTFor(fm.getRemoteFracs(), &fm.oldestCTRemote, "remote")
}

func (fm *FracManager) updateOldestCTFor(
	fracs List, v *atomic.Uint64, storageType string,
) {
	oldestByCT := fracs.GetOldestFrac()

	if oldestByCT == nil {
		v.Store(0)
		return
	}

	newOldestCT := oldestByCT.Info().CreationTime
	prevOldestCT := v.Swap(newOldestCT)

	if newOldestCT != prevOldestCT {
		logger.Info(
			"new oldest by creation time",
			zap.String("fraction", oldestByCT.Info().Name()),
			zap.String("storage_type", storageType),
			zap.Time("creation_time", time.UnixMilli(int64(newOldestCT))),
		)
	}
}

func (fm *FracManager) shiftFirstFrac() frac.Fraction {
	fm.fracMu.Lock()
	defer fm.fracMu.Unlock()

	if len(fm.localFracs) == 0 {
		return nil
	}

	outsider := fm.localFracs[0].instance
	fm.localFracs[0] = nil
	fm.localFracs = fm.localFracs[1:]
	return outsider
}

// removeStaleFractions removes [frac.Remote] fractions from external storage.
// Decision is based on the retention period provided by user.
func (fm *FracManager) removeStaleFractions(cleanupWg *sync.WaitGroup, retention time.Duration) {
	// User did not provide retention period so keep all remote fractions alive.
	// It's safe to do because we do not keep anything locally (but maybe we will eventually run out of inodes).
	if retention <= 0 {
		return
	}

	var (
		staleFractions []*frac.Remote
		freshFractions []*frac.Remote
	)

	fm.fracMu.Lock()

	for _, f := range fm.remoteFracs {
		ct := time.UnixMilli(int64(f.Info().CreationTime))
		if time.Since(ct) < retention {
			freshFractions = append(freshFractions, f)
			continue
		}
		staleFractions = append(staleFractions, f)
	}

	fm.remoteFracs = freshFractions

	fm.fracMu.Unlock()

	cleanupWg.Add(1)
	go func() {
		defer cleanupWg.Done()

		for _, f := range staleFractions {
			ct := time.UnixMilli(int64(f.Info().CreationTime))

			logger.Info(
				"removing stale remote fraction",
				zap.String("fraction", f.Info().Name()),
				zap.Time("creation_time", ct),
				zap.String("retention", retention.String()),
			)

			fm.fracCache.Remove(f.Info().Name())
			f.Suicide()
		}
	}()
}

func (fm *FracManager) Flags() *StateManager {
	return fm.flags
}

func (fm *FracManager) determineOutsiders() []frac.Fraction {
	var outsiders []frac.Fraction

	localFracs := fm.getLocalFracs()
	occupiedSize := localFracs.GetTotalSize()

	var truncated int
	for occupiedSize > fm.config.TotalSize {
		outsider := fm.shiftFirstFrac()
		if outsider == nil {
			break
		}

		localFracs = localFracs[1:]
		outsiders = append(outsiders, outsider)
		occupiedSize -= outsider.Info().FullSize()
		truncated++
	}

	if len(outsiders) > 0 && !fm.flags.IsCapacityExceeded() {
		if err := fm.flags.setCapacityExceeded(true); err != nil {
			logger.Fatal("set capacity exceeded error", zap.Error(err))
		}
	}

	metric.MaintenanceTruncateTotal.Add(float64(truncated))
	return outsiders
}

func (fm *FracManager) cleanupFractions(cleanupWg *sync.WaitGroup) {
	outsiders := fm.determineOutsiders()
	if len(outsiders) == 0 {
		return
	}

	for _, outsider := range outsiders {
		cleanupWg.Add(1)
		go func() {
			defer cleanupWg.Done()

			info := outsider.Info()
			if !fm.config.OffloadingEnabled {
				fm.fracCache.Remove(info.Name())
				outsider.Suicide()
				return
			}

			offloadStart := time.Now()
			remote, err := fm.fracProvider.Offload(fm.ctx, outsider)
			if err != nil {
				metric.OffloadingTotal.WithLabelValues("failure").Inc()
				metric.OffloadingDurationSeconds.Observe(float64(time.Since(offloadStart).Seconds()))

				logger.Error(
					"will call Suicide() on fraction: failed to offload fraction",
					zap.String("fraction", info.Name()),
					zap.Int("retry_count", fm.s3cli.MaxRetryAttempts()),
					zap.Error(err),
				)

				fm.fracCache.Remove(info.Name())
				outsider.Suicide()

				return
			}

			if remote == nil {
				fm.fracCache.Remove(info.Name())
				outsider.Suicide()
				return
			}

			metric.OffloadingTotal.WithLabelValues("success").Inc()
			metric.OffloadingDurationSeconds.Observe(float64(time.Since(offloadStart).Seconds()))

			logger.Info(
				"successully offloaded fraction",
				zap.String("fraction", info.Name()),
				zap.String("took", time.Since(offloadStart).String()),
			)

			fm.fracMu.Lock()
			// FIXME(dkharms): We had previously shifted fraction from local fracs list (in [fm.determineOutsiders] via [fm.shiftFirstFrac])
			// and therefore excluded it from search queries.
			// But now we return that fraction back (well now it's a [frac.Remote] fraction but it still points to the same data)
			// so user can face incosistent search results.
			fm.remoteFracs = append(fm.remoteFracs, remote)
			fm.fracMu.Unlock()

			outsider.Suicide()
		}()
	}
}

// Fractions returns a list of known fracs (local and remote).
//
// While working with this list, it may become irrelevant (factions may, for example, be deleted).
// This is a valid situation, because access to the data of these factions (search and fetch) occurs under blocking (see DataProvider).
// This way we avoid the race.
//
// Accessing the deleted faction data just will return an empty result.
func (fm *FracManager) Fractions() (fracs List) {
	return append(fm.getLocalFracs(), fm.getRemoteFracs()...)
}

func (fm *FracManager) getLocalFracs() List {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	fracs := make(List, 0, len(fm.localFracs))
	for _, f := range fm.localFracs {
		fracs = append(fracs, f.instance)
	}

	return fracs
}

func (fm *FracManager) getRemoteFracs() List {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	fracs := make(List, 0, len(fm.remoteFracs))
	for _, f := range fm.remoteFracs {
		fracs = append(fracs, f)
	}

	return fracs
}

func (fm *FracManager) processFracsStats() {
	type fracStats struct {
		docsTotal uint64
		docsRaw   uint64
		docsDisk  uint64
		index     uint64
		totalSize uint64
		count     int
	}

	calculate := func(fracs List) (st fracStats) {
		for _, f := range fracs {
			info := f.Info()
			st.count += 1
			st.totalSize += info.FullSize()
			st.docsTotal += uint64(info.DocsTotal)
			st.docsRaw += info.DocsRaw
			st.docsDisk += info.DocsOnDisk
			st.index += info.IndexOnDisk + info.MetaOnDisk
		}
		return
	}

	setMetrics := func(st string, oldest uint64, ft fracStats) {
		logger.Info("fraction stats",
			zap.Int("count", ft.count),
			zap.String("storage_type", st),
			zap.Uint64("docs_k", ft.docsTotal/1000),
			util.ZapUint64AsSizeStr("total_size", ft.totalSize),
			util.ZapUint64AsSizeStr("docs_raw", ft.docsRaw),
			util.ZapUint64AsSizeStr("docs_comp", ft.docsDisk),
			util.ZapUint64AsSizeStr("index", ft.index),
		)

		metric.DataSizeTotal.WithLabelValues("total", st).Set(float64(ft.totalSize))
		metric.DataSizeTotal.WithLabelValues("docs_raw", st).Set(float64(ft.docsRaw))
		metric.DataSizeTotal.WithLabelValues("docs_on_disk", st).Set(float64(ft.docsDisk))
		metric.DataSizeTotal.WithLabelValues("index", st).Set(float64(ft.index))

		if oldest != 0 {
			metric.OldestFracTime.WithLabelValues(st).
				Set((time.Duration(oldest) * time.Millisecond).Seconds())
		}
	}

	setMetrics("local", fm.oldestCTLocal.Load(), calculate(fm.getLocalFracs()))
	setMetrics("remote", fm.oldestCTRemote.Load(), calculate(fm.getRemoteFracs()))
}

func (fm *FracManager) runMaintenanceLoop(ctx context.Context) {
	fm.mntcWG.Add(1)
	go func() {
		defer fm.mntcWG.Done()

		var (
			sealWg    sync.WaitGroup
			cleanupWg sync.WaitGroup
		)

		util.RunEvery(ctx.Done(), fm.config.MaintenanceDelay, func() {
			fm.maintenance(&sealWg, &cleanupWg)
		})

		sealWg.Wait()
		cleanupWg.Wait()
	}()
}

func (fm *FracManager) runStatsLoop(ctx context.Context) {
	fm.statWG.Add(1)
	go func() {
		defer fm.statWG.Done()

		util.RunEvery(ctx.Done(), time.Second*10, func() {
			fm.processFracsStats()
		})
	}()
}

func (fm *FracManager) Start() {
	var ctx context.Context
	ctx, fm.stopFn = context.WithCancel(fm.ctx)

	fm.runStatsLoop(ctx)
	fm.runMaintenanceLoop(ctx)
	startCacheWorker(ctx, fm.config, fm.cacheMaintainer, &fm.cacheWG)
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

func (fm *FracManager) load(ctx context.Context) error {
	l := NewLoader(fm.config, fm.fracProvider, fm.fracCache)

	active, locals, remotes, err := l.Load(ctx)
	if err != nil {
		return err
	}

	for _, s := range locals {
		fm.localFracs = append(fm.localFracs, &fracRef{instance: s})
	}

	for _, s := range remotes {
		fm.remoteFracs = append(fm.remoteFracs, s)
	}

	fm.active = fm.newActiveRef(active)
	fm.localFracs = append(fm.localFracs, fm.active.ref)

	fm.updateOldestCT()
	return nil
}

func (fm *FracManager) Append(ctx context.Context, docs, metas storage.DocBlock) error {
	var err error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err = fm.Writer().Append(docs, metas); err == nil {
				return nil
			}
			logger.Info("append fail", zap.Error(err)) // can get fail if fraction already sealed
		}
	}
}

func (fm *FracManager) seal(activeRef activeRef) {
	sealsTotal.Inc()
	now := time.Now()
	sealed, err := activeRef.frac.Seal()
	if err != nil {
		if errors.Is(err, ErrSealingFractionSuicided) {
			// the faction is suicided, this means that it has already pushed out of the list of factions,
			// so we simply skip further actions
			return
		}
		logger.Fatal("sealing error", zap.Error(err))
	}
	sealingTime := time.Since(now)
	sealsDoneSeconds.Observe(sealingTime.Seconds())

	logger.Info(
		"fraction sealed",
		zap.String("fraction", filepath.Dir(sealed.Info().Path)),
		zap.Float64("time_spent_s", util.DurationToUnit(sealingTime, "s")),
	)

	info := sealed.Info()
	fm.fracCache.Add(info)

	fm.fracMu.Lock()
	activeRef.ref.instance = sealed
	fm.fracMu.Unlock()
}

func (fm *FracManager) rotate() activeRef {
	next := fm.newActiveRef(fm.fracProvider.CreateActive())

	fm.fracMu.Lock()
	prev := fm.active
	fm.active = next
	fm.localFracs = append(fm.localFracs, fm.active.ref)
	fm.fracMu.Unlock()

	logger.Info("new fraction created", zap.String("filepath", next.frac.active.BaseFileName))

	return prev
}

func (fm *FracManager) minFracSizeToSeal() uint64 {
	return fm.config.FracSize * fm.config.MinSealFracSize / 100
}

func (fm *FracManager) Stop() {
	fm.Writer().WaitWriteIdle()
	fm.indexer.Stop()
	fm.stopFn()

	fm.statWG.Wait()
	fm.mntcWG.Wait()
	fm.cacheWG.Wait()

	if err := fm.fracCache.SyncWithDisk(); err != nil {
		logger.Error(
			"failed to sync frac-cache on disk",
			zap.Error(err),
		)
	}

	needSealing := false
	status := "frac too small to be sealed"

	info := fm.active.frac.Info()
	if info.FullSize() > fm.minFracSizeToSeal() {
		needSealing = true
		status = "need seal active fraction before exit"
	}

	logger.Info(
		"sealing on exit",
		zap.String("status", status),
		zap.String("frac", info.Name()),
		zap.Uint64("fill_size_mb", uint64(util.SizeToUnit(info.FullSize(), "mb"))),
	)

	if needSealing {
		fm.seal(fm.active)
	}
}

func (fm *FracManager) Writer() *proxyFrac {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	return fm.active.frac
}

func (fm *FracManager) Active() frac.Fraction {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	return fm.active.frac
}
