package asyncsearcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/metric"
)

var (
	asyncSearchActiveSearches = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "in_progress",
		Help:      "Number of active async searches in progress",
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
		Name:      "disk_usage_bytes",
		Help:      "Disk space used by async search files in bytes by file type",
	}, []string{"file_type"})
	asyncSearchDiskUsageLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "disk_usage_limit_bytes",
		Help:      "Maximum disk space async searches is allowed to use in bytes",
	})
	asyncSearchStoredRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "stored_requests",
		Help:      "Number of stored async search requests",
	})
	asyncSearchReadOnly = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "read_only",
		Help:      "Indicates if store is in async search read-only mode (can not start a new async search) due to disk usage limits",
	})
	asyncSearchPanics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "panics_total",
		Help:      "Number of panics in async search",
	})

	ExportDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "async_search",
		Name:      "export_duration_seconds",
		Help:      "Time taken to export data by protocol in seconds",
		Buckets:   metric.SecondsBucketsDoublePrecision,
	}, []string{"protocol"})
	ExportSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "async_search",
		Name:      "export_size_bytes",
		Help:      "Size of exported data by protocol in bytes",
		Buckets:   prometheus.ExponentialBuckets(10, 3, 20),
	}, []string{"protocol"})
	CurrentExportersCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "async_search",
		Name:      "export_current_exporters_in_progress",
		Help:      "Current number of active exporters in progress by protocol",
	}, []string{"protocol"})
)
