package indexer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/metric"
)

var (
	bulkParseDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "parse_duration_seconds",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	})

	notAnObjectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "not_an_object_errors_total",
		Help:      "Number of ingestion errors due to incorrect document type",
	})

	bulkTimeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "time_errors_total",
		Help:      "errors for time rules violation in events",
	}, []string{"cause"})

	bulkSizeAfterCompression = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "size_after_compression_bytes",
		Help:      "Bulk request sizes after compression",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 16),
	})
)
