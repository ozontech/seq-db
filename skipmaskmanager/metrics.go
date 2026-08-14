package skipmaskmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	inProgress = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "skip_masks",
		Name:      "in_progress",
		Help:      "Number of skip masks in progress",
	})
	diskUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "skip_masks",
		Name:      "disk_usage_bytes",
		Help:      "Disk space used by skip mask files in bytes",
	})
	stored = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "skip_masks",
		Name:      "stored",
		Help:      "Number of active skip masks",
	})
)
