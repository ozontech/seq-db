package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/buildinfo"
)

var (
	Version = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Name:      "version",
		Help:      "",
	},
		[]string{"version"})

	RepetitionsDocsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "merge",
		Name:      "repetitions_docs_total",
		Help:      "",
	})

	CountersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "counters_total",
		Help:      "",
	}, []string{"name"})

	// SecondsBuckets covers range from 1ms to 177s.
	SecondsBuckets = prometheus.ExponentialBuckets(0.001, 3, 12)
	// SecondsBucketsDelay covers range from 1min to 2048min.
	SecondsBucketsDelay = prometheus.ExponentialBuckets(60, 2, 12)
	// SecondsRanges covers range from 1min to 47day.
	SecondsRanges = prometheus.ExponentialBuckets(60, 2.1, 16)
)

func init() {
	Version.WithLabelValues(buildinfo.Version).Inc()
}
