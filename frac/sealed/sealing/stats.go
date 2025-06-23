package sealing

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

type blocksStats struct {
	start       time.Time
	len         int
	rawLen      int
	blocksCount int
}

func startStats() blocksStats {
	return blocksStats{start: time.Now()}
}

func (s *blocksStats) takeStock(block indexBlock) {
	s.blocksCount++
	s.len += len(block.payload)
	s.rawLen += block.rawLen
}

func (s *blocksStats) log(name string, endTime time.Time) {
	var ratio float64
	if s.len > 0 {
		ratio = float64(s.rawLen) / float64(s.len)
	}
	logger.Info("seal block stats",
		zap.String("type", name),
		util.ZapUint64AsSizeStr("raw", uint64(s.rawLen)),
		util.ZapUint64AsSizeStr("compressed", uint64(s.len)),
		util.ZapFloat64WithPrec("ratio", ratio, 2),
		zap.Uint64("blocks_count", uint64(s.blocksCount)),
		util.ZapDurationWithPrec("write_duration_ms", endTime.Sub(s.start), "ms", 0),
	)
}
