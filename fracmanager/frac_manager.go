package fracmanager

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/oklog/ulid/v2"

	"github.com/ozontech/seq-db/conf"
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

	readLimiter  *disk.ReadLimiter
	indexWorkers *frac.IndexWorkers

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
	frac *frac.Active
}

// NewFracManagerWithBackgroundStart only used from tests
func NewFracManagerWithBackgroundStart(config *Config) (*FracManager, error) {
	fracManager := NewFracManager(config)
	if err := fracManager.Load(context.Background()); err != nil {
		return nil, err
	}
	fracManager.Start()

	return fracManager, nil
}

func NewFracManager(config *Config) *FracManager {
	FillConfigWithDefault(config)

	indexWorkers := frac.NewIndexWorkers(conf.IndexWorkers, conf.IndexWorkers)

	fracManager := &FracManager{
		config:       config,
		mature:       atomic.Bool{},
		indexWorkers: indexWorkers,
		readLimiter:  disk.NewReadLimiter(conf.ReaderWorkers, metric.StoreBytesRead),
		ulidEntropy:  ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
		cacheMaintainer: NewCacheMaintainer(config.CacheSize, &CacheMaintainerMetrics{
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
		}),
		fracCache: NewSealedFracCache(filepath.Join(config.DataDir, consts.FracCacheFileSuffix)),
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
	if fm.GetActiveFrac().Info().DocsOnDisk > fm.config.FracSize {
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
		size -= outsider.FullSize()
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
func (fm *FracManager) GetAllFracs() frac.List {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	fracs := make(frac.List, len(fm.fracs))
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
		totalSize += f.FullSize()
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
	fm.indexWorkers.Start() // first start indexWorkers to allow active frac replaying

	var err error
	var notSealed []activeRef

	l := NewLoader(fm.config, fm.readLimiter, fm.cacheMaintainer, fm.indexWorkers, fm.fracCache)
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

func (fm *FracManager) Append(ctx context.Context, docs, metas disk.DocBlock, writeQueue *atomic.Uint64) error {
	for {
		select {
		case <-ctx.Done():
			writeQueue.Dec()
			return ctx.Err()
		default:
			if err := fm.GetActiveFrac().Append(docs, metas, writeQueue); err != nil { // can get fail if fraction already sealed
				logger.Info("append fail", zap.Error(err))
				continue
			}
		}
		break
	}

	return nil
}

func (fm *FracManager) seal(activeRef activeRef) {
	indexFile, err := activeRef.frac.Seal(fm.config.SealParams)
	if err != nil {
		logger.Panic("sealing error", zap.Error(err))
	}
	sealed := frac.NewSealedFromActive(activeRef.frac, fm.readLimiter, indexFile, fm.cacheMaintainer.CreateIndexCache())

	stats := sealed.Info()
	fm.fracCache.AddFraction(stats.Name(), stats)

	fm.fracMu.Lock()
	activeRef.ref.instance = sealed
	fm.fracMu.Unlock()

	activeRef.frac.Release(sealed)
}

func (fm *FracManager) rotate() activeRef {
	filePath := fileBasePattern + fm.nextFractionID()
	baseFilePath := filepath.Join(fm.config.DataDir, filePath)
	logger.Info("creating new fraction", zap.String("filepath", baseFilePath))

	fm.fracMu.Lock()
	defer fm.fracMu.Unlock()

	prev := fm.active

	active := frac.NewActive(baseFilePath, fm.config.ShouldRemoveMeta, fm.indexWorkers, fm.readLimiter, fm.cacheMaintainer.CreateDocBlockCache())
	fm.active.frac = active
	fm.active.ref = &fracRef{instance: active}
	fm.fracs = append(fm.fracs, fm.active.ref)

	return prev
}

func (fm *FracManager) shouldSealOnExit(active *frac.Active) bool {
	minSize := float64(fm.config.FracSize) * consts.SealOnExitFracSizePercent / 100
	return active.FullSize() > uint64(minSize)
}

func (fm *FracManager) Stop() {
	fm.indexWorkers.Stop()
	fm.stopFn()

	fm.statWG.Wait()
	fm.mntcWG.Wait()
	fm.cacheWG.Wait()

	n := fm.active.frac.Info().Name()
	s := uint64(util.SizeToUnit(fm.active.frac.FullSize(), "mb"))

	if fm.shouldSealOnExit(fm.active.frac) {
		logger.Info("start sealing fraction on exit", zap.String("frac", n), zap.Uint64("fill_size_mb", s))
		fm.seal(fm.active)
	} else {
		logger.Info("frac too small to be sealed on exit", zap.String("frac", n), zap.Uint64("fill_size_mb", s))
	}
}

func (fm *FracManager) GetActiveFrac() *frac.Active {
	fm.fracMu.RLock()
	defer fm.fracMu.RUnlock()

	return fm.active.frac
}

func (fm *FracManager) WaitIdle() {
	fm.GetActiveFrac().WaitWriteIdle()
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
