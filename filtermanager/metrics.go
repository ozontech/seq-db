package filtermanager

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
		Help:      "Disk space used by filter files in bytes",
	})
	storedFilters = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "filters",
		Name:      "stored",
		Help:      "Number of active doc filters",
	})
)
