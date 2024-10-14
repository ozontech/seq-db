package fracmanager

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

type layerInfo struct {
	size    *atomic.Uint64
	metrics *cache.Metrics
}

type CacheMaintainer struct {
	info                   map[string]layerInfo
	lidsCleaner            *cache.Cleaner // long slices, big total size in bytes
	tokensCleaner          *cache.Cleaner // lots of entries, often accessed
	otherCleaner           *cache.Cleaner // params, index, mids, rids: not too much in total
	lidsSizeLimit          uint64
	tokensSizeLimit        uint64
	lidsAndTokensSizeLimit uint64
	otherSizeLimit         uint64
	totalSizeLimit         uint64
	docBlockSizeLimit      uint64
	docBlockCleaner        *cache.Cleaner
	metrics                *SealedIndexCacheMetrics
}

const (
	docBlockName = "docblock"
	midsName     = "mids"
	tokensName   = "tokens"
	lidsName     = "lids"
	paramsName   = "params"
	ridsName     = "rids"
	indexName    = "index"
)

var cacheLayerNames = []string{midsName, tokensName, lidsName, paramsName, ridsName, indexName, docBlockName}

func NewCacheMaintainer(sealedIndexCacheSize, docBlockCacheSize uint64, metrics *SealedIndexCacheMetrics) *CacheMaintainer {
	info := make(map[string]layerInfo)
	for _, name := range cacheLayerNames {
		s := &atomic.Uint64{}
		i := layerInfo{size: s}
		if metrics != nil {
			i.metrics = metrics.GetLayerMetrics(name)
		}
		info[name] = i
	}
	// legacy: as cache doesn't block on data insertion, and doesn't control
	// data before entering cache, and data still in-use after being evicted from cache
	// we need some margin. It was always 20%. May be reduced depending on performance
	// or removed if we manage allocations through cache
	s := float64(sealedIndexCacheSize) * 0.8
	return &CacheMaintainer{
		info: info,
		// index cache settings
		lidsCleaner:   cache.NewCleaner([]*atomic.Uint64{info[lidsName].size}),
		tokensCleaner: cache.NewCleaner([]*atomic.Uint64{info[tokensName].size}),
		otherCleaner: cache.NewCleaner([]*atomic.Uint64{
			info[midsName].size,
			info[paramsName].size,
			info[ridsName].size,
			info[indexName].size,
		}),
		// none two of three can completely purge the third cache
		// these limitations fix ranges of sizes they may take
		// lids will have from 0.4 to 0.7 of total size (1.0 - 0.4 - 0.2 = 0.4)
		// tokens will have from 0.1 to 0.4 (1.0 - 0.7 - 0.2 = 0.1)
		// other will be between 0.1 and 0.2 (1.0 - 0.9 = 0.1)
		// thus, each cache is protected from complete purge by others
		// and still there's some room for self-balancing
		lidsSizeLimit:          uint64(s * 0.7),
		tokensSizeLimit:        uint64(s * 0.4),
		lidsAndTokensSizeLimit: uint64(s * 0.9),
		otherSizeLimit:         uint64(s * 0.2),
		totalSizeLimit:         uint64(s),

		// doc block cache settings
		docBlockCleaner:   cache.NewCleaner([]*atomic.Uint64{info[docBlockName].size}),
		docBlockSizeLimit: docBlockCacheSize,

		metrics: metrics,
	}
}

func newCache[V any](cm *CacheMaintainer, layerName string) *cache.Cache[V] {
	info := cm.info[layerName]
	return cache.NewCache[V](info.size, info.metrics)
}

func (cm *CacheMaintainer) CreateDocBlockCache() *cache.Cache[[]byte] {
	newDocCache := newCache[[]byte](cm, docBlockName)

	cm.docBlockCleaner.AddBuckets(newDocCache)
	return newDocCache
}

func (cm *CacheMaintainer) CreateSealedIndexCache() *frac.SealedIndexCache {
	b := &frac.SealedIndexCache{
		MIDs:     newCache[[]byte](cm, midsName),
		Tokens:   newCache[*token.CacheEntry](cm, tokensName),
		LIDs:     newCache[*lids.Chunks](cm, lidsName),
		Params:   newCache[[]uint64](cm, paramsName),
		RIDs:     newCache[[]byte](cm, ridsName),
		Registry: newCache[[]byte](cm, indexName),
	}
	cm.lidsCleaner.AddBuckets(b.LIDs)
	cm.tokensCleaner.AddBuckets(b.Tokens)
	cm.otherCleaner.AddBuckets(b.MIDs, b.Params, b.RIDs, b.Registry)
	return b
}

func (cm *CacheMaintainer) GetSize() uint64 {
	size := uint64(0)
	for _, info := range cm.info {
		size += info.size.Load()
	}
	return size
}

func (cm *CacheMaintainer) RunCleanLoop(done <-chan struct{}, interval time.Duration) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		util.RunEvery(done, interval, func() {
			cm.CleanUp()
		})
	}()
	return wg
}

func clean(toSize uint64, stat *cache.CleanStat, cleaners ...*cache.Cleaner) {
	var sizes []uint64
	var totalSize uint64
	for _, c := range cleaners {
		s := c.GetSize()
		sizes = append(sizes, s)
		totalSize += s
	}
	if totalSize <= toSize {
		return
	}
	ratio := float64(totalSize) / float64(toSize)
	for i, c := range cleaners {
		c.CleanUp(uint64(float64(sizes[i])/ratio), stat)
	}
}

func (cm *CacheMaintainer) CleanUp() {
	t1 := time.Now()
	fromSize := cm.GetSize()
	stat := &cache.CleanStat{}

	clean(cm.lidsSizeLimit, stat, cm.lidsCleaner)
	clean(cm.tokensSizeLimit, stat, cm.tokensCleaner)
	clean(cm.otherSizeLimit, stat, cm.otherCleaner)
	clean(cm.lidsAndTokensSizeLimit, stat, cm.lidsCleaner, cm.tokensCleaner)
	clean(cm.totalSizeLimit, stat, cm.lidsCleaner, cm.tokensCleaner, cm.otherCleaner)

	clean(cm.docBlockSizeLimit, stat, cm.docBlockCleaner)

	if stat.Bytes != 0 {
		newestCreationTime := int64(0)
		oldestCreationTime := int64(math.MaxInt64)
		newestCreationTime, oldestCreationTime = cm.lidsCleaner.GetCreationTimes(newestCreationTime, oldestCreationTime)
		newestCreationTime, oldestCreationTime = cm.tokensCleaner.GetCreationTimes(newestCreationTime, oldestCreationTime)
		newestCreationTime, oldestCreationTime = cm.otherCleaner.GetCreationTimes(newestCreationTime, oldestCreationTime)

		t2 := time.Now()
		logger.Info("cache cleaned up",
			util.ZapUint64AsSizeStr("from", fromSize),
			util.ZapUint64AsSizeStr("released", stat.Bytes),
			zap.Uint64("buckets", stat.Buckets),
			zap.Int("generations", stat.Generations),
			util.ZapDurationWithPrec("min_age_s", time.Duration(t2.UnixNano()-newestCreationTime), "s", 3),
			util.ZapDurationWithPrec("max_age_s", time.Duration(t2.UnixNano()-oldestCreationTime), "s", 3),
			util.ZapDurationWithPrec("took_ms", t2.Sub(t1), "ms", 2),
		)
	}
}

// Reset is used in tests only
func (cm *CacheMaintainer) Reset() {
	cm.lidsCleaner.Reset()
	cm.tokensCleaner.Reset()
	cm.otherCleaner.Reset()
}

func (cm *CacheMaintainer) ReportStats() {
	totalSize := cm.GetSize()
	overcommitted := uint64(0)
	if totalSize > cm.totalSizeLimit {
		overcommitted = cm.totalSizeLimit - totalSize
	}

	logger.Info("cache stats",
		util.ZapUint64AsSizeStr("total_size", totalSize),
		util.ZapUint64AsSizeStr("overcommitted", overcommitted),
	)

	for _, name := range cacheLayerNames {
		size := cm.info[name].size.Load()
		cm.metrics.SizeTotal.WithLabelValues(name).Set(float64(size))
		logger.Info("layer info",
			zap.String("name", name),
			util.ZapUint64AsSizeStr("size", size),
		)
	}
}

type SealedIndexCacheMetrics struct {
	TouchTotal        *prometheus.CounterVec
	HitsTotal         *prometheus.CounterVec
	MissTotal         *prometheus.CounterVec
	PanicsTotal       *prometheus.CounterVec
	LockWaitsTotal    *prometheus.CounterVec
	WaitsTotal        *prometheus.CounterVec
	ReattemptsTotal   *prometheus.CounterVec
	HitsSizeTotal     *prometheus.CounterVec
	MissSizeTotal     *prometheus.CounterVec
	SizeReleasedTotal *prometheus.CounterVec

	SizeTotal *prometheus.GaugeVec
}

func (m *SealedIndexCacheMetrics) GetLayerMetrics(layerName string) *cache.Metrics {
	return &cache.Metrics{
		TouchTotal:        m.TouchTotal.WithLabelValues(layerName),
		HitsTotal:         m.HitsTotal.WithLabelValues(layerName),
		MissTotal:         m.MissTotal.WithLabelValues(layerName),
		PanicsTotal:       m.PanicsTotal.WithLabelValues(layerName),
		LockWaitsTotal:    m.LockWaitsTotal.WithLabelValues(layerName),
		WaitsTotal:        m.WaitsTotal.WithLabelValues(layerName),
		ReattemptsTotal:   m.ReattemptsTotal.WithLabelValues(layerName),
		HitsSizeTotal:     m.HitsSizeTotal.WithLabelValues(layerName),
		MissSizeTotal:     m.MissSizeTotal.WithLabelValues(layerName),
		SizeReleasedTotal: m.SizeReleasedTotal.WithLabelValues(layerName),
	}
}
