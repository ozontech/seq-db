package fracmanager

import (
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// fracsStats contains statistical information about a group of fractions
// Used to track aggregate metrics for fractions in different states
type fracsStats struct {
	count           int    // Number of fractions in the group
	docsCount       uint64 // Total number of documents across all fractions
	docsSizeRaw     uint64 // Total raw size of documents before compression
	docsSizeOnDisk  uint64 // Total size of documents on disk after compression
	indexSizeOnDisk uint64 // Total size of index and metadata on disk
	totalSizeOnDisk uint64 // Total storage size, including documents, index and metadata
}

var (
	dataSizeTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "common",
		Name:      "data_size_total",
		Help:      "",
	}, []string{"kind", "storage_type"})
)

// Add incorporates fraction information into the statistics
// Updates all aggregate metrics with the values from the provided fraction info
func (s *fracsStats) Add(info *common.Info) {
	s.count++
	s.docsCount += uint64(info.DocsTotal)
	s.docsSizeRaw += info.DocsRaw
	s.docsSizeOnDisk += info.DocsOnDisk
	s.indexSizeOnDisk += info.IndexOnDisk + info.MetaOnDisk
	s.totalSizeOnDisk += info.FullSize()
}

// Sub removes fraction information from the statistics
// Decrements all aggregate metrics with the values from the provided fraction info
func (s *fracsStats) Sub(info *common.Info) {
	s.count--
	s.docsCount -= uint64(info.DocsTotal)
	s.docsSizeRaw -= info.DocsRaw
	s.docsSizeOnDisk -= info.DocsOnDisk
	s.indexSizeOnDisk -= info.IndexOnDisk + info.MetaOnDisk
	s.totalSizeOnDisk -= info.FullSize()
}

func (s *fracsStats) Log(stage string) {
	logger.Info("fraction stats",
		zap.Int("count", s.count),
		zap.String("stage", stage),
		zap.Uint64("docs_k", s.docsCount/1000),
		util.ZapUint64AsSizeStr("total_size", s.totalSizeOnDisk),
		util.ZapUint64AsSizeStr("docs_raw", s.docsSizeRaw),
		util.ZapUint64AsSizeStr("docs_comp", s.docsSizeOnDisk),
		util.ZapUint64AsSizeStr("index", s.indexSizeOnDisk),
	)
}

func (s *fracsStats) SetMetrics(metric *prometheus.GaugeVec, stage string) {
	metric.WithLabelValues("total", stage).Set(float64(s.totalSizeOnDisk))
	metric.WithLabelValues("docs_raw", stage).Set(float64(s.docsSizeRaw))
	metric.WithLabelValues("docs_on_disk", stage).Set(float64(s.docsSizeOnDisk))
	metric.WithLabelValues("index", stage).Set(float64(s.indexSizeOnDisk))
}

// registryStats contains statistical data for all fraction queues
// Used for monitoring and memory management decisions
type registryStats struct {
	sealing    fracsStats // Statistics for fractions in the sealing process
	locals     fracsStats // Statistics for fractions on local disk
	offloading fracsStats // Statistics for fractions in the offloading process
	remotes    fracsStats // Statistics for fractions in remote storage
}

func (s *registryStats) Log() {
	s.sealing.Log("sealing")
	s.locals.Log("locals")
	s.offloading.Log("offloading")
	s.remotes.Log("remotes")
}

func (s *registryStats) SetMetrics() {
	s.sealing.SetMetrics(dataSizeTotal, "sealing")
	s.locals.SetMetrics(dataSizeTotal, "locals")
	s.offloading.SetMetrics(dataSizeTotal, "offloading")
	s.remotes.SetMetrics(dataSizeTotal, "remotes")
}
