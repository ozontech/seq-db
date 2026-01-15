package asyncsearcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	asyncSearchActiveSearches = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "in_progress",
		Help:      "Amount of active async searches in progress",
	})
	asyncSearchConcurrencyLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "concurrency_limit",
		Help:      "Maximum number of simultaneously running async searches",
	})
	asyncSearchDiskUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "disk_usage_bytes_total",
	}, []string{"file_type"})
	asyncSearchDiskUsageLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "disk_usage_limit_bytes",
	})
	asyncSearchStoredRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "stored_requests",
	})
	asyncSearchReadOnly = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "read_only",
	})
)
