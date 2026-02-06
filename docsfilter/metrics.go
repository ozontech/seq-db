package docsfilter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	inProgressFilters = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "in_progress",
		Help:      "Number of doc filters in progress",
	})
	diskUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "disk_usage_bytes",
	})
	storedFilters = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "stored",
		Help:      "Number of active doc filters",
	})
)
