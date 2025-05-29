package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	BulkDiskSyncTasksCount = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "disk_sync_tasks_count",
		Help:      "",
		Buckets:   prometheus.LinearBuckets(1, 16, 16),
	})

	CacheOldest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "oldest",
		Help:      "",
	}, []string{"cleaner"})
	CacheAddBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "add_buckets",
		Help:      "",
	}, []string{"cleaner"})
	CacheDelBuckets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "del_buckets",
		Help:      "",
	}, []string{"cleaner"})
	CacheCleanGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "clean_generations",
		Help:      "",
	}, []string{"cleaner"})
	CacheChangeGenerations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "change_generations",
		Help:      "",
	}, []string{"cleaner"})

	CacheSizeReleased = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "size_released_total",
		Help:      "",
	}, []string{"layer"})
	CacheHitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_total",
		Help:      "",
	}, []string{"layer"})
	CacheMissTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_total",
		Help:      "",
	}, []string{"layer"})
	CachePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "panics_total",
		Help:      "",
	}, []string{"layer"})
	CacheLockWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "lock_waits_total",
		Help:      "",
	}, []string{"layer"})
	CacheWaitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "waits_total",
		Help:      "",
	}, []string{"layer"})
	CacheReattemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "reattempts_total",
		Help:      "",
	}, []string{"layer"})
	CacheSizeRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_size_total",
		Help:      "",
	}, []string{"layer"})
	CacheSizeOccupied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_size_total",
		Help:      "",
	}, []string{"layer"})
	CacheMapsRecreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "maps_recreated",
		Help:      "",
	}, []string{"layer"})
	CacheMissLatencySec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "miss_latency_sec",
		Help:      "",
	}, []string{"layer"})

	DataSizeTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "data_size_total",
		Help:      "",
	}, []string{"kind"})

	OldestFracTime = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "oldest_frac_time",
		Help:      "",
	})

	BulkDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	BulkDuplicateDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "duplicate_docs_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "docs_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkDocBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "doc_bytes_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkMetaBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "meta_bytes_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})

	SearchInFlightQueriesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "in_flight_queries_total",
		Help:      "",
	})
	RejectedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "rejected_requests",
		Help:      "",
	}, []string{"method", "type"})
	SearchDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	SearchTreeDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tree_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})

	SearchRangesSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "ranges_seconds",
		Help:      "",
		Buckets:   SecondsRanges,
	})
	FetchInFlightQueriesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "in_flight_queries_total",
		Help:      "",
	})
	FetchDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	FetchDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "docs_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 32),
	})
	FetchDocsNotFound = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "docs_not_found",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 32),
	})
	FetchBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "bytes_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 32),
	})
	FetchActiveStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "active_stages_seconds",
		Buckets:   SecondsBuckets,
	}, []string{"stage"})
	FetchSealedStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "sealed_stages_seconds",
		Buckets:   SecondsBuckets,
	}, []string{"stage"})
	MaintenanceTruncateTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "maintanance",
		Name:      "truncate_total",
		Help:      "",
	})

	StoreReady = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "ready",
		Help:      "store is ready to accept requests",
	})

	FractionLoadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "fraction_load_errors",
		Help:      "Doc file load errors (missing or invalid doc file)",
	})

	StorePanics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "panics_total",
		Help:      "",
	})

	skippedIndexes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "skipped_indexes",
		Help:      "",
	}, []string{"type"})
	SkippedIndexesText    = skippedIndexes.WithLabelValues("text")
	SkippedIndexesKeyword = skippedIndexes.WithLabelValues("keyword")
	SkippedIndexesPath    = skippedIndexes.WithLabelValues("path")

	skippedIndexesBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "skipped_indexes_bytes",
		Help:      "",
	}, []string{"type"})
	SkippedIndexesBytesText    = skippedIndexesBytes.WithLabelValues("text")
	SkippedIndexesBytesKeyword = skippedIndexesBytes.WithLabelValues("keyword")
	SkippedIndexesBytesPath    = skippedIndexesBytes.WithLabelValues("path")
)
