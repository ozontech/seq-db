package fracmanager

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

const (
	midsName       = "mids"
	lidsName       = "lids"
	paramsName     = "params"
	ridsName       = "rids"
	indexName      = "index"
	tokensName     = "tokens"
	tokenTableName = "token_table"
	docsName       = "docblock"
)

type cleanerConf struct {
	layers    []string
	sizeLimit uint64 // either fixed size
	weight    uint64 // or relative size
}

var config = []cleanerConf{
	{
		layers: []string{indexName},
		weight: 3,
	},
	{
		layers: []string{midsName, ridsName, paramsName},
		weight: 8,
	},
	{
		layers: []string{tokenTableName},
		weight: 8,
	},
	{
		layers: []string{tokensName},
		weight: 36,
	},
	{
		layers: []string{lidsName},
		weight: 37,
	},
	{
		layers: []string{docsName},
		weight: 8,
	},
}

type CacheMaintainer struct {
	layers        map[string]layer
	cleaners      []*cache.Cleaner
	cleanerLabels []string
}

type layer struct {
	metrics *cache.Metrics
	cleaner *cache.Cleaner
}

func createCleaners(cfg []cleanerConf, totalSize uint64, metrics *CacheMaintainerMetrics) ([]*cache.Cleaner, []string) {
	labels := make([]string, len(cfg))
	cleaners := make([]*cache.Cleaner, len(cfg))

	s := float64(totalSize) * 0.9 // reserve 10% for spikes

	totalWeights := 0
	for i := range cfg {
		s -= float64(cfg[i].sizeLimit)
		totalWeights += int(cfg[i].weight)
	}

	for i, cfgItem := range cfg {
		sizeLimit := cfgItem.sizeLimit
		if sizeLimit == 0 {
			sizeLimit = uint64(s * float64(cfgItem.weight) / float64(totalWeights))
		}
		labels[i] = strings.Join(cfgItem.layers, "-")
		cleaners[i] = cache.NewCleaner(sizeLimit, metrics.GetCleanerMetrics(labels[i]))
	}

	return cleaners, labels
}

func cleanersToLayers(cfg []cleanerConf, cleaners []*cache.Cleaner, metrics *CacheMaintainerMetrics) map[string]layer {
	res := map[string]layer{}
	for i, c := range cfg {
		for _, l := range c.layers {
			var m *cache.Metrics
			if metrics != nil {
				m = metrics.GetLayerMetrics(l)
			}
			res[l] = layer{
				metrics: m,
				cleaner: cleaners[i],
			}
		}
	}
	return res
}

func NewCacheMaintainer(totalCacheSize uint64, metrics *CacheMaintainerMetrics) *CacheMaintainer {
	cleaners, labels := createCleaners(config, totalCacheSize, metrics)
	return &CacheMaintainer{
		cleanerLabels: labels,
		cleaners:      cleaners,
		layers:        cleanersToLayers(config, cleaners, metrics),
	}
}

func newCache[V any](cm *CacheMaintainer, layerName string) *cache.Cache[V] {
	c := cm.layers[layerName]
	return cache.NewCache[V](c.cleaner, c.metrics)
}

func (cm *CacheMaintainer) CreateDocBlockCache() *cache.Cache[[]byte] {
	return newCache[[]byte](cm, docsName)
}

func (cm *CacheMaintainer) CreateIndexCache() *frac.IndexCache {
	return &frac.IndexCache{
		MIDs:       newCache[[]byte](cm, midsName),
		RIDs:       newCache[[]byte](cm, ridsName),
		Params:     newCache[[]uint64](cm, paramsName),
		LIDs:       newCache[*lids.Chunks](cm, lidsName),
		Tokens:     newCache[*token.CacheEntry](cm, tokensName),
		TokenTable: newCache[token.Table](cm, tokenTableName),
		Registry:   newCache[[]byte](cm, indexName),
	}
}

func (cm *CacheMaintainer) RunCleanLoop(done <-chan struct{}, cleanupInterval, gcInterval time.Duration) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		runs := 0
		gcRunsCount := int(gcInterval / cleanupInterval)

		util.RunEvery(done, cleanupInterval, func() {
			runs++
			cm.rotate()
			cm.cleanup()

			if runs >= gcRunsCount {
				runs = 0
				cm.garbageCollection()
			}
		})
	}()
	return wg
}

func (cm *CacheMaintainer) rotate() {
	for _, cleaner := range cm.cleaners {
		cleaner.Rotate()
	}
}

func (cm *CacheMaintainer) cleanup() {
	for i, cleaner := range cm.cleaners {
		start := time.Now()
		stat := &cache.CleanStat{}
		if cleaner.Cleanup(stat) {
			stop := time.Now()
			logger.Info("cache cleaner old generations cleaned up",
				zap.String("name", cm.cleanerLabels[i]),
				util.ZapUint64AsSizeStr("from_size", stat.TotalSize),
				util.ZapUint64AsSizeStr("to_size", cleaner.SizeLimit()),
				util.ZapUint64AsSizeStr("size_to_clean", stat.SizeToClean),
				util.ZapUint64AsSizeStr("released", stat.BytesReleased),
				zap.Int("buckets_cleaned", stat.BucketsCleaned),
				zap.Int("buckets_total", stat.BucketsTotal),
				zap.Int("gen_cleaned", stat.GensCleaned),
				zap.Int("gen_total", stat.GensTotal),
				util.ZapDurationWithPrec("age_s", time.Duration(stop.UnixNano()-stat.OldestGenTime), "s", 2),
				util.ZapDurationWithPrec("took_ms", stop.Sub(start), "ms", 2),
			)
		}
	}
}

func (cm *CacheMaintainer) garbageCollection() {
	for i, cleaner := range cm.cleaners {
		if cleaned := cleaner.CleanEmptyGenerations(); cleaned > 0 {
			logger.Info("cache cleaner empty generations cleaned",
				zap.String("name", cm.cleanerLabels[i]),
				zap.Int("generations_deleted", cleaned),
			)
		}

		if released := cleaner.ReleaseBuckets(); released > 0 {
			logger.Info("cache cleaner buckets released",
				zap.String("name", cm.cleanerLabels[i]),
				zap.Int("buckets_deleted", released),
			)
		}
	}
}

// Reset is used in tests only
func (cm *CacheMaintainer) Reset() {
	for _, cleaner := range cm.cleaners {
		cleaner.Reset()
	}
}

type CacheMaintainerMetrics struct {
	HitsTotal       *prometheus.CounterVec
	MissTotal       *prometheus.CounterVec
	PanicsTotal     *prometheus.CounterVec
	LockWaitsTotal  *prometheus.CounterVec
	WaitsTotal      *prometheus.CounterVec
	ReattemptsTotal *prometheus.CounterVec
	SizeRead        *prometheus.CounterVec
	SizeOccupied    *prometheus.CounterVec
	SizeReleased    *prometheus.CounterVec
	MapsRecreated   *prometheus.CounterVec
	MissLatency     *prometheus.CounterVec

	Oldest            *prometheus.GaugeVec
	AddBuckets        *prometheus.CounterVec
	DelBuckets        *prometheus.CounterVec
	CleanGenerations  *prometheus.CounterVec
	ChangeGenerations *prometheus.CounterVec
}

func (m *CacheMaintainerMetrics) GetLayerMetrics(layerName string) *cache.Metrics {
	return &cache.Metrics{
		HitsTotal:       m.HitsTotal.WithLabelValues(layerName),
		MissTotal:       m.MissTotal.WithLabelValues(layerName),
		PanicsTotal:     m.PanicsTotal.WithLabelValues(layerName),
		LockWaitsTotal:  m.LockWaitsTotal.WithLabelValues(layerName),
		WaitsTotal:      m.WaitsTotal.WithLabelValues(layerName),
		ReattemptsTotal: m.ReattemptsTotal.WithLabelValues(layerName),
		SizeRead:        m.SizeRead.WithLabelValues(layerName),
		SizeOccupied:    m.SizeOccupied.WithLabelValues(layerName),
		SizeReleased:    m.SizeReleased.WithLabelValues(layerName),
		MapsRecreated:   m.MapsRecreated.WithLabelValues(layerName),
		MissLatency:     m.MissLatency.WithLabelValues(layerName),
	}
}

func (m *CacheMaintainerMetrics) GetCleanerMetrics(cleanerLabel string) *cache.CleanerMetrics {
	if m == nil {
		return nil
	}
	return &cache.CleanerMetrics{
		Oldest:            m.Oldest.WithLabelValues(cleanerLabel),
		AddBuckets:        m.AddBuckets.WithLabelValues(cleanerLabel),
		DelBuckets:        m.DelBuckets.WithLabelValues(cleanerLabel),
		CleanGenerations:  m.CleanGenerations.WithLabelValues(cleanerLabel),
		ChangeGenerations: m.ChangeGenerations.WithLabelValues(cleanerLabel),
	}
}
