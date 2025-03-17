package searcher

import (
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
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
	metric.SearchLeavesTotal.Observe(float64(s.LeavesTotal))
	metric.SearchNodesTotal.Observe(float64(s.NodesTotal))
	metric.SearchSourcesTotal.Observe(float64(s.SourcesTotal))
	metric.SearchAggNodesTotal.Observe(float64(s.AggNodesTotal))
	metric.SearchHitsTotal.Observe(float64(s.HitsTotal))
}

func chooseStagesMetric(fracType string, hasAgg, hasHist bool) *prometheus.HistogramVec {
	if fracType == frac.TypeActive {
		if hasAgg {
			return metric.ActiveAggSearchSec
		}
		if hasHist {
			return metric.ActiveHistSearchSec
		}
		return metric.ActiveRegSearchSec
	}
	if hasAgg {
		return metric.SealedAggSearchSec
	}
	if hasHist {
		return metric.SealedHistSearchSec
	}
	return metric.SealedRegSearchSec
}
