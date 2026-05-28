package compaction

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/metric"
)

var (
	compactionInflight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "compaction",
		Name:      "inflight",
		Help:      "Number of running compactions",
	})

	compactionSkipped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "compaction",
		Name:      "skipped_total",
		Help:      "Tick-triggered tasks dropped because all workers were busy or no candidates were found",
	})

	compactionBinsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "compaction",
		Name:      "bins_total",
		Help:      "Number of active time-bins considered for compaction",
	})

	compactionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "compaction",
		Name:      "duration_seconds",
		Help:      "Time spent executing a single compaction",
		Buckets:   metric.SecondsBuckets,
	})

	compactionBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "compaction",
		Name:      "bytes_total",
		Help:      "Total index bytes merged across all compactions",
	})

	compactionResultTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "compaction",
		Name:      "result_total",
		Help:      "Compaction outcomes by result (success, empty, error)",
	}, []string{"result"})
)
