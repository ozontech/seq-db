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
		Name:      "oldest_timestamp_seconds",
		Help:      "Unix timestamp in seconds for the oldest cache entry",
	}, []string{"cleaner"})
	cacheAddBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "add_buckets_total",
		Help:      "",
	}, []string{"cleaner"})
	cacheDelBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "del_buckets_total",
		Help:      "",
	}, []string{"cleaner"})
	cacheCleanGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "clean_generations_total",
		Help:      "",
	}, []string{"cleaner"})
	cacheChangeGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "change_generations_total",
		Help:      "",
	}, []string{"cleaner"})
	cacheSizeReleased = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "size_released_bytes_total",
		Help:      "Total size in bytes released from cache",
	}, []string{"layer"})
	cacheHitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_total",
		Help:      "",
	}, []string{"layer"})
	cacheMissTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_total",
		Help:      "",
	}, []string{"layer"})
	cachePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "panics_total",
		Help:      "",
	}, []string{"layer"})
	cacheLockWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "lock_waits_total",
		Help:      "",
	}, []string{"layer"})
	cacheWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "waits_total",
		Help:      "",
	}, []string{"layer"})
	cacheReattemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "reattempts_total",
		Help:      "",
	}, []string{"layer"})
	cacheSizeRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_size_bytes_total",
		Help:      "Total size in bytes read from cache hits",
	}, []string{"layer"})
	cacheSizeOccupied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_size_bytes_total",
		Help:      "Total size in bytes occupied by cache misses",
	}, []string{"layer"})
	cacheMapsRecreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "maps_recreated_total",
		Help:      "",
	}, []string{"layer"})
	cacheMissLatencySec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_latency_seconds_total",
		Help:      "",
	}, []string{"layer"})

	// fetcher's metrics
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "stages_seconds",
		Buckets:   metric.SecondsBuckets,
		Help:      "",
	}, []string{"stage"})
	fetcherIDsPerFraction = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "ids_per_fraction",
		Help:      "",
	})
	fetcherWithHints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_with_hints_total",
		Help:      "",
	})
	fetcherWithoutHint = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_without_hints_total",
		Help:      "",
	})
	fetcherHintMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "hint_misses_total",
		Help:      "",
	})

	// searchers's metrics
	searchSubSearches = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sub_searches",
		Buckets:   []float64{0.99, 1, 1.01, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048},
		Help:      "",
	})

	// fraction lifecycle metrics
	sealsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "main",
		Name:      "seals_total",
		Help:      "",
	})
	sealsDoneSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db",
		Subsystem: "main",
		Name:      "seals_done_seconds",
	})
	maintenanceTruncateTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "maintenance",
		Name:      "fractions_truncated_total",
		Help:      "How many fractions were truncated",
	})
	offloadingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "offloading",
		Name:      "fractions_total",
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
		Name:      "data_size_bytes",
		Help:      "Total data (fractions) size in bytes",
	}, []string{"kind", "storage_type"})
	oldestFracTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "oldest_fraction_timestamp_seconds",
		Help:      "Unix timestamp in seconds of the oldest fraction",
	}, []string{"storage_type"})
	fractionLoadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "fraction_load_errors_total",
		Help:      "Doc file load errors (missing or invalid doc file)",
	})

	// disk load metric
	storeBytesRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "bytes_read_total",
		Help:      "",
	})
)
