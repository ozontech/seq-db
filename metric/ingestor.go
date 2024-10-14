package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	SearchOverall = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "total",
		Help:      "",
	})
	SearchColdTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "cold_total",
		Help:      "",
	})
	SearchColdErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "cold_errors_total",
		Help:      "",
	})
	SearchErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "errors_total",
		Help:      "",
	})
	IngestorPanics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "common",
		Name:      "panics_total",
		Help:      "",
	})

	SearchPartial = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "partial_total",
		Help:      "Number of searches ending with partial response",
	})

	// Subsystem: "bulk"

	IngestorBulkRequestPoolDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "request_pool_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkParseDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "parse_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkDocProvideDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "doc_provide_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkSendAttemptDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "send_attempt_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkAttemptErrorDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "attempt_error_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkSkipCold = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "skip_cold_total",
		Help:      "",
	})
	IngestorBulkSkipShard = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "skip_shard_total",
		Help:      "",
	})
	BulkErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "errors_total",
		Help:      "",
	})

	// Subsystem: "fetch"

	DocumentsFetched = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "total_fetched_documents",
		Help:      "Number of documents returned by the Fetch method",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	DocumentsRequested = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "total_requested_documents",
		Help:      "Number of documents requested using the Fetch method",
		Buckets:   prometheus.ExponentialBuckets(1, 3, 16),
	})
	FetchErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "errors_total",
		Help:      "",
	})
	FetchNotFoundError = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "not_found_errors",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 3, 16),
	})
	FetchDuplicateErrors = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "duplicate_errors",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 3, 16),
	})
	RateLimiterSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "ratelimiter",
		Name:      "map_size",
		Help:      "Size of internal map of rate limiter",
	})

	CircuitBreakerSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "circuit_breaker",
		Name:      "success",
		Help:      "Count of each time `Execute` does not return an error",
	}, []string{"name"})
	CircuitBreakerErr = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "circuit_breaker",
		Name:      "err",
		Help:      "The number of errors that have occurred in the circuit breaker",
	}, []string{"name", "kind"})
	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "circuit_breaker",
		Name:      "state",
		Help:      "The state of the circuit breaker",
	}, []string{"name"})

	ExportDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "export",
		Name:      "duration",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 25),
	}, []string{"protocol"})
	ExportSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "export",
		Name:      "size",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 50),
	}, []string{"protocol"})
	CurrentExportersCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "export",
		Name:      "current_exporters_count",
		Help:      "",
	}, []string{"protocol"})

	// Subsystem: tokenizer

	TokenizerTokensPerMessage = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "tokenizer",
		Name:      "tokens_per_message",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	}, []string{"tokenizer"})
	TokenizerParseDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "tokenizer",
		Name:      "parse_duration_seconds",
		Help:      "",
		Buckets:   SecondsBuckets,
	}, []string{"tokenizer"})
	TokenizerIncomingTextLen = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "tokenizer",
		Name:      "incoming_text_len",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
)
