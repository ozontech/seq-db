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
		Help:      "Bulk processing time",
		Buckets:   SecondsBuckets,
	})
	BulkDuplicateDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "duplicate_docs",
		Help:      "Number of duplicate documents found in active fraction in bulk requests",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "docs",
		Help:      "Number of documents in bulk request",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkDocBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "doc_bytes",
		Help:      "Byte size of document in bulk requests",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})
	BulkMetaBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "meta_bytes",
		Help:      "Size of metadata in bulk requests in bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 16),
	})

	SearchInFlightQueriesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "in_flight",
		Help:      "Current number of search requests being processed",
	})
	RejectedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "rejected_requests_total",
		Help:      "Number of rejected requests by method and reason",
	}, []string{"method", "type"})
	SearchDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "duration_seconds",
		Help:      "Search request duration time (only successful searches)",
		Buckets:   SecondsBuckets,
	})
	SearchQprMemSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "qpr_mem_size_bytes",
		Help:      "QPR heap memory size in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 3, 21),
	})

	SearchRangesSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "ranges_seconds",
		Help:      "Time range covered by search requests in seconds",
		Buckets:   SecondsRanges,
	})
	FetchInFlightQueriesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "in_flight",
		Help:      "Current number of fetch requests being processed",
	})
	FetchDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "duration_seconds",
		Help:      "Fetch requests duration time",
		Buckets:   SecondsBuckets,
	})
	FetchDocsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "docs",
		Help:      "Number of documents returned per fetch request",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 18),
	})
	FetchDocsNotFound = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "docs_not_found",
		Help:      "Number of documents not found per fetch request",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 18),
	})
	FetchBytesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetch",
		Name:      "bytes",
		Help:      "Total size of data in bytes returned per fetch request",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 20),
	})

	StoreReady = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "ready",
		Help:      "Indicates if a store is ready to accept requests",
	})

	StorePanics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "panics_total",
		Help:      "Number of panics in store",
	})

	skippedIndexes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "skipped_indexes_total",
		Help:      "Number of doc fields skipped for indexing by index type",
	}, []string{"type"})
	SkippedIndexesText    = skippedIndexes.WithLabelValues("text")
	SkippedIndexesKeyword = skippedIndexes.WithLabelValues("keyword")
	SkippedIndexesPath    = skippedIndexes.WithLabelValues("path")

	skippedIndexesBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Name:      "skipped_indexes_bytes_total",
		Help:      "Total size in bytes of doc fields skipped for indexing by index type",
	}, []string{"type"})
	SkippedIndexesBytesText    = skippedIndexesBytes.WithLabelValues("text")
	SkippedIndexesBytesKeyword = skippedIndexesBytes.WithLabelValues("keyword")
	SkippedIndexesBytesPath    = skippedIndexesBytes.WithLabelValues("path")
)
