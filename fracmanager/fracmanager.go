package fracmanager

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

const (
	fileBasePattern  = "seq-db-"
	fileImmatureFlag = ".immature"
)

type FracManager struct {
	ctx    context.Context
	config *Config

	cacheMaintainer *CacheMaintainer

	fracCache *sealedFracCache

	fracMu      sync.RWMutex
	localFracs  []*fracRef
	remoteFracs []*frac.Remote
	active      activeRef

	fracProvider *fractionProvider

	oldestCTLocal  atomic.Uint64
	oldestCTRemote atomic.Uint64
	mature         atomic.Bool

	stopFn  func()
	statWG  sync.WaitGroup
	mntcWG  sync.WaitGroup
	cacheWG *sync.WaitGroup

	s3cli *s3.Client

	ulidEntropy io.Reader
}

type fracRef struct {
	instance frac.Fraction
}

type activeRef struct {
	ref  *fracRef // ref contains a back reference to the fraction in the slice
	frac *proxyFrac
}

func (fm *FracManager) newActiveRef(active *frac.Active) activeRef {
	f := &proxyFrac{active: active, fp: fm.fracProvider}
	return activeRef{
		frac: f,
		ref:  &fracRef{instance: f},
	}
}

func NewFracManager(ctx context.Context, cfg *Config, s3cli *s3.Client) *FracManager {
	FillConfigWithDefault(cfg)

	cacheMaintainer := NewCacheMaintainer(cfg.CacheSize, cfg.SortCacheSize, &CacheMaintainerMetrics{
		HitsTotal:       metric.CacheHitsTotal,
		MissTotal:       metric.CacheMissTotal,
		PanicsTotal:     metric.CachePanicsTotal,
		LockWaitsTotal:  metric.CacheLockWaitsTotal,
		WaitsTotal:      metric.CacheWaitsTotal,
		ReattemptsTotal: metric.CacheReattemptsTotal,
		SizeRead:        metric.CacheSizeRead,
		SizeOccupied:    metric.CacheSizeOccupied,
		SizeReleased:    metric.CacheSizeReleased,
		MapsRecreated:   metric.CacheMapsRecreated,
		MissLatency:     metric.CacheMissLatencySec,

		Oldest:            metric.CacheOldest,
		AddBuckets:        metric.CacheAddBuckets,
		DelBuckets:        metric.CacheDelBuckets,
		CleanGenerations:  metric.CacheCleanGenerations,
		ChangeGenerations: metric.CacheChangeGenerations,
	})

	fracManager := &FracManager{
		config:          cfg,
		ctx:             ctx,
		s3cli:           s3cli,
		mature:          atomic.Bool{},
		cacheMaintainer: cacheMaintainer,
		fracProvider:    newFractionProvider(&cfg.Fraction, s3cli, cacheMaintainer, config.ReaderWorkers, config.IndexWorkers),
		ulidEntropy:     ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
		fracCache:       NewSealedFracCache(filepath.Join(cfg.DataDir, consts.FracCacheFileSuffix)),
	}

	return fracManager
}

// This method is not thread safe. Use consciously to avoid race
func (fm *FracManager) nextFractionID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), fm.ulidEntropy).String()
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

func (fm *FracManager) OldestCT() uint64 {
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

			fm.fracCache.RemoveFraction(f.Info().Name())
			f.Suicide()
		}
	}()
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

	if len(outsiders) > 0 && !fm.Mature() {
		fm.setMature()
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
				fm.fracCache.RemoveFraction(info.Name())
				outsider.Suicide()
				return
			}

			offloadStart := time.Now()
			mustBeOffloaded, err := outsider.Offload(fm.ctx, s3.NewUploader(fm.s3cli))
			if err != nil {
				metric.OffloadingTotal.WithLabelValues("failure").Inc()
				metric.OffloadingDurationSeconds.Observe(float64(time.Since(offloadStart).Seconds()))

				logger.Error(
					"will call Suicide() on fraction: failed to offload fraction",
					zap.String("fraction", info.Name()),
					zap.Int("retry_count", fm.s3cli.MaxRetryAttempts()),
					zap.Error(err),
				)

				fm.fracCache.RemoveFraction(info.Name())
				outsider.Suicide()

				return
			}

			if !mustBeOffloaded {
				fm.fracCache.RemoveFraction(info.Name())
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

			remote := fm.fracProvider.NewRemote(fm.ctx, info.Path, info)

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

type FracType int

const (
	FracTypeLocal FracType = 1 << iota
	FracTypeRemote
)

// GetAllFracs returns a list of known fracs (local and remote).
//
// While working with this list, it may become irrelevant (factions may, for example, be deleted).
// This is a valid situation, because access to the data of these factions (search and fetch) occurs under blocking (see DataProvider).
// This way we avoid the race.
//
// Accessing the deleted faction data just will return an empty result.
func (fm *FracManager) GetAllFracs() (fracs List) {
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
	fm.cacheWG = fm.cacheMaintainer.RunCleanLoop(ctx.Done(), fm.config.CacheCleanupDelay, fm.config.CacheGCDelay)
}

func (fm *FracManager) Load(ctx context.Context) error {
	l := NewLoader(fm.config, fm.fracProvider, fm.fracCache)

	actives, sealed, remote, err := l.load(ctx)
	if err != nil {
		return err
	}

	for _, s := range sealed {
		fm.localFracs = append(fm.localFracs, &fracRef{instance: s})
	}

	for _, s := range remote {
		fm.remoteFracs = append(fm.remoteFracs, s)
	}

	if err := fm.replayAll(ctx, actives); err != nil {
		return err
	}

	if len(fm.localFracs)+len(fm.remoteFracs) == 0 { // no data, first run
		if err := fm.setImmature(); err != nil {
			return err
		}
	} else {
		if err := fm.checkIsImmature(); err != nil {
			return err
		}
	}

	if fm.active.ref == nil { // no active
		_ = fm.rotate() // make new empty active
	}

	fm.updateOldestCT()
	return nil
}

func (fm *FracManager) replayAll(ctx context.Context, actives []*frac.Active) error {
	for i, a := range actives {
		if err := a.Replay(ctx); err != nil {
			return err
		}

		if a.Info().DocsTotal == 0 {
			a.Suicide() // remove empty
			continue
		}

		r := fm.newActiveRef(a)
		fm.localFracs = append(fm.localFracs, r.ref)

		if i == len(actives)-1 { // last and not empty
			fm.active = r
			continue
		}

		fm.seal(r)
	}

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

var (
	sealsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "main",
		Name:      "seals_total",
	})
	sealsDoneSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db",
		Subsystem: "main",
		Name:      "seals_done_seconds",
	})
)

func (fm *FracManager) seal(activeRef activeRef) {
	sealsTotal.Inc()
	now := time.Now()
	defer func() {
		sealsDoneSeconds.Observe(time.Since(now).Seconds())
	}()

	sealed, err := activeRef.frac.Seal(fm.config.SealParams)
	if err != nil {
		if errors.Is(err, ErrSealingFractionSuicided) {
			// the faction is suicided, this means that it has already pushed out of the list of factions,
			// so we simply skip further actions
			return
		}
		logger.Fatal("sealing error", zap.Error(err))
	}

	info := sealed.Info()
	fm.fracCache.AddFraction(info.Name(), info)

	fm.fracMu.Lock()
	activeRef.ref.instance = sealed
	fm.fracMu.Unlock()
}

func (fm *FracManager) rotate() activeRef {
	filePath := fileBasePattern + fm.nextFractionID()
	baseFilePath := filepath.Join(fm.config.DataDir, filePath)
	logger.Info("creating new fraction", zap.String("filepath", baseFilePath))

	next := fm.newActiveRef(fm.fracProvider.NewActive(baseFilePath))

	fm.fracMu.Lock()
	prev := fm.active
	fm.active = next
	fm.localFracs = append(fm.localFracs, fm.active.ref)
	fm.fracMu.Unlock()

	return prev
}

func (fm *FracManager) minFracSizeToSeal() uint64 {
	return fm.config.FracSize * consts.SealOnExitFracSizePercent / 100
}

func (fm *FracManager) Stop() {
	fm.fracProvider.Stop()
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

func (fm *FracManager) WaitIdle() {
	fm.Writer().WaitWriteIdle()
}

func (fm *FracManager) setMature() {
	if err := os.Remove(filepath.Join(fm.config.DataDir, fileImmatureFlag)); err != nil {
		logger.Panic(err.Error())
	}
	fm.mature.Store(true)
}

func (fm *FracManager) setImmature() error {
	fm.mature.Store(false)
	_, err := os.Create(filepath.Join(fm.config.DataDir, fileImmatureFlag))
	return err
}

func (fm *FracManager) checkIsImmature() error {
	_, err := os.Stat(filepath.Join(fm.config.DataDir, fileImmatureFlag))
	if err == nil { // file exists; store is immature
		fm.mature.Store(false)
		return nil
	}
	if os.IsNotExist(err) { // file not exists; store is mature
		fm.mature.Store(true)
		return nil
	}
	return err
}

func (fm *FracManager) Mature() bool {
	return fm.mature.Load()
}

func (fm *FracManager) SealForcedForTests() {
	active := fm.rotate()
	if active.frac.Info().DocsTotal > 0 {
		fm.seal(active)
	}
}

func (fm *FracManager) OffloadForcedForTests() {
	if !(fm.config.OffloadingEnabled && fm.config.OffloadingForced) {
		panic("trying to force offloading when it is disabled")
	}

	// Offloading works only for sealed fractions.
	fm.SealForcedForTests()

	var wg sync.WaitGroup
	fm.cleanupFractions(&wg)
	wg.Wait()
}

func (fm *FracManager) ResetCacheForTests() {
	fm.cacheMaintainer.Reset()
}
