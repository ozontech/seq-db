package fracmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/metric"
)

var (
	// cache's metrics
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

	// fetcher's metrics
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	fetcherIDsPerFraction = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "ids_per_fraction",
	})
	fetcherWithHints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_with_hints",
	})
	fetcherWithoutHint = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_without_hints",
	})
	fetcherHintMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "hint_misses",
	})

	// searchers's metrics
	searchSubSearches = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sub_searches",
		Buckets:   []float64{0.99, 1, 1.01, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048},
	})

	// fraction lifecycle metrics
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
	maintenanceTruncateTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "maintenance",
		Name:      "truncate_total",
		Help:      "",
	})
	offloadingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "offloading",
		Name:      "total",
		Help:      "How many fractions were offloaded",
	}, []string{"status"})
	offloadingDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "offloading",
		Name:      "duration_seconds",
		Help:      "How many seconds it took to offload fraction to remote storage",
		Buckets:   metric.SecondsBuckets,
	})
	dataSizeTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "data_size_total",
	}, []string{"kind", "storage_type"})
	oldestFracTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "oldest_frac_time",
		Help:      "",
	}, []string{"storage_type"})
	fractionLoadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "fraction_load_errors",
		Help:      "Doc file load errors (missing or invalid doc file)",
	})

	// disk load metric
	storeBytesRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "bytes_read",
	})
)
