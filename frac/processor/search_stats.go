package processor

import (
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	metricsNamespace = "seq_db_store"
	metricsSubsystem = "search"
)

var (
	searchLeavesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "leaves",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		Help:      "Number of leaf nodes in search query tree per search",
	})
	searchNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "nodes",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		Help:      "Number of nodes in search query tree per search",
	})
	lidsLoadedTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lids_loaded",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 20),
		Help:      "Number of LIDs accessed per search",
	})
	searchAggNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "agg_nodes",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		Help:      "Number of aggregation nodes in search query tree per search",
	})
	searchHitsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "hits",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 18),
		Help:      "Number of document hits per search",
	})
)

type searchStats struct {
	LeavesTotal     int
	NodesTotal      int
	LIDsLoadedTotal int
	HitsTotal       int
	AggNodesTotal   int
	TreeDuration    time.Duration
}

func (s *searchStats) String() string {
	res, _ := json.MarshalIndent(s, "", "\t")
	return string(res)
}

func (s *searchStats) AddLIDsCount(v int) {
	s.LIDsLoadedTotal += v
}

func (s *searchStats) UpdateMetrics() {
	searchLeavesTotal.Observe(float64(s.LeavesTotal))
	searchNodesTotal.Observe(float64(s.NodesTotal))
	lidsLoadedTotal.Observe(float64(s.LIDsLoadedTotal))
	searchAggNodesTotal.Observe(float64(s.AggNodesTotal))
	searchHitsTotal.Observe(float64(s.HitsTotal))
}
