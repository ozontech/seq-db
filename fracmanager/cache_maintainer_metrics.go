package fracmanager

import (
	"github.com/prometheus/client_golang/prometheus"

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
