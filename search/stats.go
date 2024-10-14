package search

import (
	"encoding/json"
	"time"
)

type Stats struct {
	FracType      string
	LeavesTotal   int
	NodesTotal    int
	SourcesTotal  int
	HitsTotal     int
	AggNodesTotal int
	TreeDuration  time.Duration
}

func NewStats(fracType string) *Stats {
	return &Stats{FracType: fracType}
}

func (s *Stats) String() string {
	res, _ := json.MarshalIndent(s, "", "\t")
	return string(res)
}

func (s *Stats) AddLIDsCount(v int) {
	s.SourcesTotal += v
}
