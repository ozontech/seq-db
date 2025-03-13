package searcher

import (
	"time"
)

type Stats struct {
	LeavesTotal   int
	NodesTotal    int
	SourcesTotal  int
	HitsTotal     int
	AggNodesTotal int
	TreeDuration  time.Duration
}

func (s *Stats) AddLIDsCount(v int) {
	s.SourcesTotal += v
}

func (s *Stats) updateMetrics() {
	searchLeavesTotal.Observe(float64(s.LeavesTotal))
	searchNodesTotal.Observe(float64(s.NodesTotal))
	searchSourcesTotal.Observe(float64(s.SourcesTotal))
	searchAggNodesTotal.Observe(float64(s.AggNodesTotal))
	searchHitsTotal.Observe(float64(s.HitsTotal))
}
