package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
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
		Name:      "duplicate_docs",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "docs",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkDocBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "doc_bytes",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkMetaBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "meta_bytes",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})

	SearchInFlightQueriesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "in_flight_queries",
		Help:      "",
	})
	RejectedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "rejected_requests_total",
		Help:      "",
	}, []string{"method", "type"})
	SearchDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "duration_seconds",
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
		Name:      "in_flight_queries",
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
		Name:      "docs",
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
		Name:      "bytes",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 32),
	})

	StoreReady = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "ready",
		Help:      "Store is ready to accept requests",
	})

	StorePanics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "panics_total",
		Help:      "Count of panics in store",
	})

	skippedIndexes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "skipped_indexes_total",
		Help:      "",
	}, []string{"type"})
	SkippedIndexesText    = skippedIndexes.WithLabelValues("text")
	SkippedIndexesKeyword = skippedIndexes.WithLabelValues("keyword")
	SkippedIndexesPath    = skippedIndexes.WithLabelValues("path")

	skippedIndexesBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "skipped_indexes_bytes_total",
		Help:      "",
	}, []string{"type"})
	SkippedIndexesBytesText    = skippedIndexesBytes.WithLabelValues("text")
	SkippedIndexesBytesKeyword = skippedIndexesBytes.WithLabelValues("keyword")
	SkippedIndexesBytesPath    = skippedIndexesBytes.WithLabelValues("path")
)
