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
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/util"
)

const (
	fileBasePattern  = "seq-db-"
	fileImmatureFlag = ".immature"
)

type FracManager struct {
	config *Config

	cacheMaintainer *CacheMaintainer

	fracCache *sealedFracCache

	fracMu sync.RWMutex
	fracs  []*fracRef
	active activeRef

	fracProvider *fractionProvider

	OldestCT atomic.Uint64
	mature   atomic.Bool

	stopFn  func()
	statWG  sync.WaitGroup
	mntcWG  sync.WaitGroup
	cacheWG *sync.WaitGroup

	ulidEntropy io.Reader
}

type fracRef struct {
	instance frac.Fraction
}

type activeRef struct {
	ref  *fracRef // ref contains a back reference to the fraction in the slice
	frac *proxyFrac
}

func NewFracManager(cfg *Config) *FracManager {
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
		mature:          atomic.Bool{},
		cacheMaintainer: cacheMaintainer,
		fracProvider:    newFractionProvider(&cfg.Fraction, cacheMaintainer, config.ReaderWorkers, config.IndexWorkers),
		ulidEntropy:     ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
		fracCache:       NewSealedFracCache(filepath.Join(cfg.DataDir, consts.FracCacheFileSuffix)),
	}

	return fracManager
}

// This method is not thread safe. Use consciously to avoid race
func (fm *FracManager) nextFractionID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), fm.ulidEntropy).String()
}

func (fm *FracManager) maintenance(sealWG, suicideWG *sync.WaitGroup) {
	logger.Debug("maintenance started")

	n := time.Now()
	if fm.Active().Info().DocsOnDisk > fm.config.FracSize {
		active := fm.rotate()

		sealWG.Add(1)
		go func() {
			fm.seal(active)
			sealWG.Done()
		}()
	}

	fm.shrinkSizes(suicideWG)

	if err := fm.fracCache.SyncWithDisk(); err != nil {
		logger.Error("can't sync frac cache", zap.Error(err))
	}

	logger.Debug("maintenance finished", zap.Int64("took_ms", time.Since(n).Milliseconds()))
}

func (fm *FracManager) shiftFirstFrac() frac.Fraction {
	fm.fracMu.Lock()
	defer fm.fracMu.Unlock()

	if len(fm.fracs) == 0 {
		return nil
	}

	outsider := fm.fracs[0].instance
	fm.fracs[0] = nil
	fm.fracs = fm.fracs[1:]
	return outsider
}

func (fm *FracManager) shrinkSizes(suicideWG *sync.WaitGroup) {
	var outsiders []frac.Fraction
	fracs := fm.GetAllFracs()
	size := fracs.GetTotalSize()

	for size > fm.config.TotalSize {
		outsider := fm.shiftFirstFrac()
		if outsider == nil {
			break
		}

		outsiders = append(outsiders, outsider)
		size -= outsider.Info().FullSize()
		fracs = fracs[1:]

		if !fm.Mature() {
			fm.setMature()
		}
		fm.fracCache.RemoveFraction(outsider.Info().Name())
		metric.MaintenanceTruncateTotal.Inc()
		logger.Info("truncating last fraction", zap.Any("fraction", outsider))
	}

	if len(outsiders) > 0 {
		suicideWG.Add(len(outsiders))

		for _, outsider := range outsiders {
			go func() {
				defer suicideWG.Done()
				outsider.Suicide()
			}()
		}
	}

	if oldestByCT := fracs.GetOldestFrac(); oldestByCT != nil {
		newOldestCT := oldestByCT.Info().CreationTime
		prevOldestCT := fm.OldestCT.Swap(newOldestCT)
		if newOldestCT != prevOldestCT {
			logger.Info("new oldest by creation time", zap.Any("fraction", oldestByCT))
		}
	}
}

// GetAllFracs returns a list of known fracs. While working with this list,
// it may become irrelevant (factions may, for example, be deleted).
// This is a valid situation, because access to the data of these factions
// (search and fetch) occurs under blocking (see DataProvider).
// This way we avoid the race.
// Accessing the deleted faction data just will return an empty result.
func (fm *FracManager) GetAllFracs() List {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	fracs := make(List, len(fm.fracs))
	for i, f := range fm.fracs {
		fracs[i] = f.instance
	}
	return fracs
}

func (fm *FracManager) processFracsStats() {
	docsTotal := uint64(0)
	docsRaw := uint64(0)
	docsDisk := uint64(0)
	index := uint64(0)
	totalSize := uint64(0)

	fracs := fm.GetAllFracs()

	for _, f := range fracs {
		info := f.Info()
		totalSize += info.FullSize()
		docsTotal += uint64(info.DocsTotal)
		docsRaw += info.DocsRaw
		docsDisk += info.DocsOnDisk
		index += info.IndexOnDisk + info.MetaOnDisk
	}

	if len(fracs) > 0 {
		logger.Info("last fraction details", zap.Any("fraction", fracs[0]))
	}

	oldestCT := fm.OldestCT.Load()

	logger.Info("fraction stats",
		zap.Int("count", len(fracs)),
		zap.Uint64("docs_k", docsTotal/1000),
		util.ZapUint64AsSizeStr("total_size", totalSize),
		util.ZapUint64AsSizeStr("docs_raw", docsRaw),
		util.ZapUint64AsSizeStr("docs_comp", docsDisk),
		util.ZapUint64AsSizeStr("index", index),
		util.ZapMsTsAsESTimeStr("oldest_ct", oldestCT),
	)

	metric.DataSizeTotal.WithLabelValues("total").Set(float64(totalSize))
	metric.DataSizeTotal.WithLabelValues("docs_raw").Set(float64(docsRaw))
	metric.DataSizeTotal.WithLabelValues("docs_on_disk").Set(float64(docsDisk))
	metric.DataSizeTotal.WithLabelValues("index").Set(float64(index))
	if oldestCT != 0 {
		metric.OldestFracTime.Set((time.Duration(oldestCT) * time.Millisecond).Seconds())
	}
}

func (fm *FracManager) runMaintenanceLoop(ctx context.Context) {
	fm.mntcWG.Add(1)
	go func() {
		defer fm.mntcWG.Done()

		sealWG := sync.WaitGroup{}
		suicideWG := sync.WaitGroup{}
		util.RunEvery(ctx.Done(), fm.config.MaintenanceDelay, func() {
			fm.maintenance(&sealWG, &suicideWG)
		})
		sealWG.Wait()
		suicideWG.Wait()
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
	ctx, fm.stopFn = context.WithCancel(context.Background())

	fm.runStatsLoop(ctx)
	fm.runMaintenanceLoop(ctx)
	fm.cacheWG = fm.cacheMaintainer.RunCleanLoop(ctx.Done(), fm.config.CacheCleanupDelay, fm.config.CacheGCDelay)
}

func (fm *FracManager) Load(ctx context.Context) error {
	var err error
	var notSealed []activeRef

	l := NewLoader(fm.config, fm.fracProvider, fm.fracCache)

	if fm.fracs, notSealed, err = l.load(ctx); err != nil {
		return err
	}

	if len(fm.fracs) == 0 { // no data, first run
		if err := fm.setImmature(); err != nil {
			return err
		}
	} else {
		if err := fm.checkIsImmature(); err != nil {
			return err
		}
	}

	if len(notSealed) == 0 {
		fm.rotate()
	} else {
		if len(notSealed) > 1 {
			logger.Info("sealing active fractions")
			for _, active := range notSealed[:len(notSealed)-1] {
				fm.seal(active)
			}
		}
		fm.active = notSealed[len(notSealed)-1]
	}

	return nil
}

func (fm *FracManager) Append(ctx context.Context, docs, metas disk.DocBlock) error {
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

	next := fm.fracProvider.newActiveRef(fm.fracProvider.NewActive(baseFilePath))

	fm.fracMu.Lock()
	prev := fm.active
	fm.active = next
	fm.fracs = append(fm.fracs, fm.active.ref)
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

func (fm *FracManager) ResetCacheForTests() {
	fm.cacheMaintainer.Reset()
}
