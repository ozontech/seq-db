package docsfilter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// nolint:unused // in progress
var (
	activeFilters = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "in_progress",
		Help:      "Number of doc filters in progress",
	})
	diskUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "disk_usage_bytes",
	}, []string{"file_type"})
	storedFilters = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "stored",
		Help:      "Number of active doc filters",
	})
)
