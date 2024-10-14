package cache

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	TouchTotal        prometheus.Counter
	HitsTotal         prometheus.Counter
	MissTotal         prometheus.Counter
	PanicsTotal       prometheus.Counter
	LockWaitsTotal    prometheus.Counter
	WaitsTotal        prometheus.Counter
	ReattemptsTotal   prometheus.Counter
	HitsSizeTotal     prometheus.Counter
	MissSizeTotal     prometheus.Counter
	SizeReleasedTotal prometheus.Counter
}
