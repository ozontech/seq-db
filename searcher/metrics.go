package searcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

var (
	activeRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_reg_search_sec",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	activeHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_hist_search_sec",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	activeAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_agg_search_sec",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

	sealedRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_reg_search_sec",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	sealedHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_hist_search_sec",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	sealedAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_agg_search_sec",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

	searchHitsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "hits_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 32),
	})
	searchLeavesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "leaves_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	searchNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "nodes_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	searchSourcesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sources_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 20),
	})
	searchAggNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "agg_nodes_total",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})

	searchSubSearches = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sub_searches",
		Help:      "",
		Buckets:   []float64{0.99, 1, 1.01, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048},
	})
	searchEvalDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "eval_duration_seconds",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	})
)

func chooseStagesMetric(fracType string, hasAgg, hasHist bool) *prometheus.HistogramVec {
	if fracType == frac.TypeActive {
		if hasAgg {
			return activeAggSearchSec
		}
		if hasHist {
			return activeHistSearchSec
		}
		return activeRegSearchSec
	}
	if hasAgg {
		return sealedAggSearchSec
	}
	if hasHist {
		return sealedHistSearchSec
	}
	return sealedRegSearchSec
}
