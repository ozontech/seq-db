package fracmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
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

func (s *fracsStats) Set(info *common.Info) {
	s.count = 1
	s.docsCount = uint64(info.DocsTotal)
	s.docsSizeRaw = info.DocsRaw
	s.docsSizeOnDisk = info.DocsOnDisk
	s.indexSizeOnDisk = info.IndexOnDisk + info.MetaOnDisk
	s.totalSizeOnDisk = info.FullSize()
}

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
	active     fracsStats // Statistics for active fraction
	sealing    fracsStats // Statistics for fractions in the sealing process
	sealed     fracsStats // Statistics for fractions on sealed disk
	compacting fracsStats // Statistics for fractions participating in compaction
	offloading fracsStats // Statistics for fractions in the offloading process
	remotes    fracsStats // Statistics for fractions in remote storage
}

func (s *registryStats) Log() {
	s.active.Log("active")
	s.sealing.Log("sealing")
	s.sealed.Log("sealed")
	s.compacting.Log("compacting")
	s.offloading.Log("offloading")
	s.remotes.Log("remotes")
}

func (s *registryStats) SetMetrics() {
	s.active.SetMetrics(dataSizeTotal, "active")
	s.sealing.SetMetrics(dataSizeTotal, "sealing")
	s.sealed.SetMetrics(dataSizeTotal, "sealed")
	s.compacting.SetMetrics(dataSizeTotal, "compacting")
	s.offloading.SetMetrics(dataSizeTotal, "offloading")
	s.remotes.SetMetrics(dataSizeTotal, "remotes")
}

func (s registryStats) TotalSizeOnDiskLocal() uint64 {
	return s.sealing.totalSizeOnDisk + s.sealed.totalSizeOnDisk + s.compacting.totalSizeOnDisk
}
