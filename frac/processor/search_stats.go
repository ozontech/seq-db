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
		Help:      "",
	})
	searchNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "nodes",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		Help:      "",
	})
	searchSourcesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "sources",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 20),
		Help:      "",
	})
	searchAggNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "agg_nodes",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		Help:      "",
	})
	searchHitsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "hits",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 32),
		Help:      "",
	})
)

type searchStats struct {
	LeavesTotal   int
	NodesTotal    int
	SourcesTotal  int
	HitsTotal     int
	AggNodesTotal int
	TreeDuration  time.Duration
}

func (s *searchStats) String() string {
	res, _ := json.MarshalIndent(s, "", "\t")
	return string(res)
}

func (s *searchStats) AddLIDsCount(v int) {
	s.SourcesTotal += v
}

func (s *searchStats) UpdateMetrics() {
	searchLeavesTotal.Observe(float64(s.LeavesTotal))
	searchNodesTotal.Observe(float64(s.NodesTotal))
	searchSourcesTotal.Observe(float64(s.SourcesTotal))
	searchAggNodesTotal.Observe(float64(s.AggNodesTotal))
	searchHitsTotal.Observe(float64(s.HitsTotal))
}
