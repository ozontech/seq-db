package cache

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	HitsTotal       prometheus.Counter
	MissTotal       prometheus.Counter
	PanicsTotal     prometheus.Counter
	LockWaitsTotal  prometheus.Counter
	WaitsTotal      prometheus.Counter
	ReattemptsTotal prometheus.Counter
	SizeRead        prometheus.Counter
	SizeOccupied    prometheus.Counter
	SizeReleased    prometheus.Counter
	MapsRecreated   prometheus.Counter
	MissLatency     prometheus.Counter
}

func (m *Metrics) reportPanic() {
	if m != nil {
		m.PanicsTotal.Inc()
	}
}

func (m *Metrics) reportLockWait() {
	if m != nil {
		m.LockWaitsTotal.Inc()
	}
}

func (m *Metrics) reportWait() {
	if m != nil {
		m.WaitsTotal.Inc()
	}
}

func (m *Metrics) reportReattempt() {
	if m != nil {
		m.ReattemptsTotal.Inc()
	}
}

func (m *Metrics) reportHits(size uint64) {
	if m != nil {
		m.HitsTotal.Inc()
		m.SizeRead.Add(float64(size))
	}
}

func (m *Metrics) reportMiss(size uint64, latencySec float64) {
	if m != nil {
		m.MissTotal.Inc()
		m.SizeOccupied.Add(float64(size))
		m.MissLatency.Add(latencySec)
	}
}

func (m *Metrics) reportReleased(freed uint64) {
	if m != nil {
		m.SizeReleased.Add(float64(freed))
	}
}

func (m *Metrics) reportMapsRecreated() {
	if m != nil {
		m.MapsRecreated.Inc()
	}
}
