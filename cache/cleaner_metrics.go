package cache

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type CleanerMetrics struct {
	Oldest prometheus.Gauge

	AddBuckets prometheus.Counter
	DelBuckets prometheus.Counter

	CleanGenerations  prometheus.Counter
	ChangeGenerations prometheus.Counter
}

func (m *CleanerMetrics) BucketsInc() {
	if m != nil {
		m.AddBuckets.Inc()
	}
}

func (m *CleanerMetrics) BucketsSub(cnt int) {
	if m != nil {
		m.DelBuckets.Add(float64(cnt))
	}
}

func (m *CleanerMetrics) GenerationsInc() {
	if m != nil {
		m.ChangeGenerations.Inc()
	}
}

func (m *CleanerMetrics) GenerationsSub(cnt int) {
	if m != nil {
		m.CleanGenerations.Add(float64(cnt))
	}
}

func (m *CleanerMetrics) OldestSet(nsec int64) {
	if m != nil {
		m.Oldest.Set(float64(time.Unix(0, nsec).Unix()))
	}
}
