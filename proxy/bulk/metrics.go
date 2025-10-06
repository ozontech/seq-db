package bulk

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	inflightBulks = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "in_flight_queries_total",
		Help:      "",
	})

	rateLimitedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Name:      "rate_limited_total",
		Help:      "Count of rate limited requests",
	})

	docsWritten = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "docs_written",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
)
