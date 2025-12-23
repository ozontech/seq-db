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
		Help:      "Number of search requests ingestor started to process",
	})
	SearchColdTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "cold_total",
		Help:      "Number of search requests sent to cold stores",
	})
	SearchColdErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "cold_errors_total",
		Help:      "Number of errors in cold search requests",
	})
	SearchErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "errors_total",
		Help:      "Number of search requests completed with error",
	})
	IngestorPanics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "common",
		Name:      "panics_total",
		Help:      "Number of panics in ingestor",
	})

	SearchPartial = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "search",
		Name:      "partial_total",
		Help:      "Number of searches ending with partial response",
	})

	// Subsystem: "bulk"

	IngestorBulkDocProvideDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "doc_provide_duration_seconds",
		Help:      "Bulk processing time (parsing, tokenization, extracting meta, compressing docs and meta) by ingestor",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkSendAttemptDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "send_attempt_duration_seconds",
		Help:      "Time spent sending a bulk to stores successfully in seconds",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkAttemptErrorDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "attempt_error_duration_seconds",
		Help:      "Time spent before error occurred in bulk send attempts in seconds",
		Buckets:   SecondsBuckets,
	})
	IngestorBulkSkipCold = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "skip_cold_total",
		Help:      "Number of bulk requests where sending to cold storage was skipped since it was already stored in cold at the previous attempt",
	})
	IngestorBulkSkipShard = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "skip_shard_total",
		Help:      "Number of replicas skipped when choosing replicas to send a bulk since the replica was chosen for storage already",
	})
	BulkErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "errors_total",
		Help:      "Number of errors in bulk processing",
	})

	// Subsystem: "fetch"

	DocumentsFetched = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "fetched_docs",
		Help:      "Number of documents returned by the Fetch method",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	DocumentsRequested = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "requested_docs",
		Help:      "Number of documents requested using the Fetch method",
		Buckets:   prometheus.ExponentialBuckets(1, 3, 16),
	})
	FetchErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "errors_total",
		Help:      "Number of errors in fetch requests",
	})
	FetchNotFoundError = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "not_found_errors",
		Help:      "Number of documents not found per fetch request",
		Buckets:   prometheus.ExponentialBuckets(1, 3, 16),
	})
	FetchDuplicateErrors = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "fetch",
		Name:      "duplicate_errors",
		Help:      "Number of duplicate document errors per fetch request",
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
		Name:      "success_total",
		Help:      "Count of each time `Execute` does not return an error",
	}, []string{"name"})
	CircuitBreakerErr = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "circuit_breaker",
		Name:      "errors_total",
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
		Name:      "duration_seconds",
		Help:      "Time taken to export data by protocol in seconds",
		Buckets:   SecondsBucketsDoublePrecision,
	}, []string{"protocol"})
	ExportSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "export",
		Name:      "size_bytes",
		Help:      "Size of exported data by protocol in bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 3, 20),
	}, []string{"protocol"})
	CurrentExportersCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "export",
		Name:      "current_exporters_in_progress",
		Help:      "Current number of active exporters in progress by protocol",
	}, []string{"protocol"})

	// Subsystem: tokenizer

	TokenizerTokensPerMessage = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "tokenizer",
		Name:      "tokens_per_message",
		Help:      "Number of tokens extracted per message by tokenizer type",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	}, []string{"tokenizer"})
	TokenizerParseDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "tokenizer",
		Name:      "parse_duration_seconds",
		Help:      "Time taken to parse and tokenize messages in seconds by tokenizer type",
		Buckets:   SecondsBuckets,
	}, []string{"tokenizer"})
	TokenizerIncomingTextLen = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "tokenizer",
		Name:      "incoming_text_len_bytes",
		Help:      "Length of incoming text for tokenization in bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
)
