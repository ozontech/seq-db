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
		Help:      "Number of cache buckets added by cleaner",
	}, []string{"cleaner"})
	cacheDelBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "del_buckets_total",
		Help:      "Number of cache buckets deleted by cleaner",
	}, []string{"cleaner"})
	cacheCleanGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "clean_generations_total",
		Help:      "Number of cache generations cleaned by cleaner",
	}, []string{"cleaner"})
	cacheChangeGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "change_generations_total",
		Help:      "Number of cache generations changed by cleaner",
	}, []string{"cleaner"})
	cacheSizeReleased = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "size_released_bytes_total",
		Help:      "Size in bytes released from cache",
	}, []string{"layer"})
	cacheHitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_total",
		Help:      "Number of cache hits by layer",
	}, []string{"layer"})
	cacheMissTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_total",
		Help:      "Number of cache misses by layer",
	}, []string{"layer"})
	cachePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "panics_total",
		Help:      "Number of panics in cache operations by layer",
	}, []string{"layer"})
	cacheLockWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "lock_waits_total",
		Help:      "Number of lock waits in cache operations by layer",
	}, []string{"layer"})
	cacheWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "waits_total",
		Help:      "Number of waits in cache operations by layer (happens when a value is being loaded concurrently by several goroutines)",
	}, []string{"layer"})
	cacheReattemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "reattempts_total",
		Help:      "Number of reattempts in cache operations by layer",
	}, []string{"layer"})
	cacheSizeRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_size_bytes_total",
		Help:      "Total size in bytes read from cache hits by layer",
	}, []string{"layer"})
	cacheSizeOccupied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_size_bytes_total",
		Help:      "Total size occupied in cache by layer in bytes",
	}, []string{"layer"})
	cacheMapsRecreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "maps_recreated_total",
		Help:      "Number of cache maps recreated by layer",
	}, []string{"layer"})
	cacheMissLatencySec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_latency_seconds_total",
		Help:      "Total latency in seconds for cache misses by layer",
	}, []string{"layer"})

	// fetcher's metrics
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "stages_seconds",
		Buckets:   metric.SecondsBuckets,
		Help:      "Fetcher stages processing time",
	}, []string{"stage"})
	fetcherIDsPerFraction = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "ids_per_fraction",
		Help:      "Number of IDs fetched per fraction",
	})
	fetcherWithHints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_with_hints_total",
		Help:      "Number of IDs with hints in fetch requests",
	})
	fetcherWithoutHint = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_without_hints_total",
		Help:      "Number of fetch requests without hints",
	})
	fetcherHintMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "hint_misses_total",
		Help:      "Number of hint misses in fetch requests",
	})

	// searchers's metrics
	searchSubSearches = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sub_searches",
		Buckets:   []float64{0.99, 1, 1.01, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048},
		Help:      "Number of sub-searches executed per search query",
	})

	// fraction lifecycle metrics
	sealsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "main",
		Name:      "seals_total",
		Help:      "Number of fraction seals performed",
	})
	sealsDoneSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db",
		Subsystem: "main",
		Name:      "seals_done_seconds",
		Help:      "Time taken to complete fraction seals in seconds",
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
		Help:      "Total data size in bytes",
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
		Help:      "Number of doc file load errors (missing or invalid doc file)",
	})

	// disk load metric
	storeBytesRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "bytes_read_total",
		Help:      "Number of bytes read from disk storage",
	})
)
