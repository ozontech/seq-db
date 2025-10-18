package fracmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
