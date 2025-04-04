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
		Name:      "leaves_total",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	searchNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "nodes_total",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
	searchSourcesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "sources_total",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 20),
	})
	searchAggNodesTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "agg_nodes_total",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	searchHitsTotal = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "hits_total",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 32),
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
