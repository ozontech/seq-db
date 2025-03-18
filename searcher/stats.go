package searcher

import (
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

var (
	SearchLeavesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "leaves_total",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	SearchNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "nodes_total",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	SearchSourcesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sources_total",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 20),
	})
	SearchAggNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "agg_nodes_total",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	SearchHitsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "hits_total",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 32),
	})

	ActiveAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	ActiveHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	ActiveRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	SealedAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	SealedHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	SealedRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

	SearchSubSearches = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "sub_searches",
		Help:      "",
		Buckets:   []float64{0.99, 1, 1.01, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048},
	})
)

type Stats struct {
	LeavesTotal   int
	NodesTotal    int
	SourcesTotal  int
	HitsTotal     int
	AggNodesTotal int
	TreeDuration  time.Duration
}

func (s *Stats) String() string {
	res, _ := json.MarshalIndent(s, "", "\t")
	return string(res)
}

func (s *Stats) AddLIDsCount(v int) {
	s.SourcesTotal += v
}

func (s *Stats) updateMetrics() {
	SearchLeavesTotal.Observe(float64(s.LeavesTotal))
	SearchNodesTotal.Observe(float64(s.NodesTotal))
	SearchSourcesTotal.Observe(float64(s.SourcesTotal))
	SearchAggNodesTotal.Observe(float64(s.AggNodesTotal))
	SearchHitsTotal.Observe(float64(s.HitsTotal))
}

func getStagesMetric(fracType string, hasAgg, hasHist bool) *prometheus.HistogramVec {
	if fracType == frac.TypeActive {
		if hasAgg {
			return ActiveAggSearchSec
		}
		if hasHist {
			return ActiveHistSearchSec
		}
		return ActiveRegSearchSec
	}
	if hasAgg {
		return SealedAggSearchSec
	}
	if hasHist {
		return SealedHistSearchSec
	}
	return SealedRegSearchSec
}
