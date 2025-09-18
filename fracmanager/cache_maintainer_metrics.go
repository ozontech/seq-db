package fracmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/cache"
)

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

var (
	cacheOldest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "oldest",
	}, []string{"cleaner"})
	cacheAddBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "add_buckets",
	}, []string{"cleaner"})
	cacheDelBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "del_buckets",
	}, []string{"cleaner"})
	cacheCleanGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "clean_generations",
	}, []string{"cleaner"})
	cacheChangeGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "change_generations",
	}, []string{"cleaner"})

	cacheSizeReleased = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "size_released_total",
	}, []string{"layer"})
	cacheHitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_total",
	}, []string{"layer"})
	cacheMissTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_total",
	}, []string{"layer"})
	cachePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "panics_total",
	}, []string{"layer"})
	cacheLockWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "lock_waits_total",
	}, []string{"layer"})
	cacheWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "waits_total",
	}, []string{"layer"})
	cacheReattemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "reattempts_total",
	}, []string{"layer"})
	cacheSizeRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_size_total",
	}, []string{"layer"})
	cacheSizeOccupied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_size_total",
	}, []string{"layer"})
	cacheMapsRecreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "maps_recreated",
	}, []string{"layer"})
	cacheMissLatencySec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_latency_sec",
	}, []string{"layer"})
)

func newDefaultCacheMetrics() *CacheMaintainerMetrics {
	return &CacheMaintainerMetrics{
		HitsTotal:       cacheHitsTotal,
		MissTotal:       cacheMissTotal,
		PanicsTotal:     cachePanicsTotal,
		LockWaitsTotal:  cacheLockWaitsTotal,
		WaitsTotal:      cacheWaitsTotal,
		ReattemptsTotal: cacheReattemptsTotal,
		SizeRead:        cacheSizeRead,
		SizeOccupied:    cacheSizeOccupied,
		SizeReleased:    cacheSizeReleased,
		MapsRecreated:   cacheMapsRecreated,
		MissLatency:     cacheMissLatencySec,

		Oldest:            cacheOldest,
		AddBuckets:        cacheAddBuckets,
		DelBuckets:        cacheDelBuckets,
		CleanGenerations:  cacheCleanGenerations,
		ChangeGenerations: cacheChangeGenerations,
	}
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
