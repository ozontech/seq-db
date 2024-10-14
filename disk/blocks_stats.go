package disk

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

type BlockStats struct {
	Name     string
	Raw      uint64
	Comp     uint64
	Blocks   uint64
	Duration time.Duration
}

func (s *BlockStats) WriteLogs() {
	ratio := float64(s.Raw) / float64(s.Comp)
	logger.Info("seal block stats",
		zap.String("type", s.Name),
		util.ZapUint64AsSizeStr("raw", s.Raw),
		util.ZapUint64AsSizeStr("compressed", s.Comp),
		util.ZapFloat64WithPrec("ratio", ratio, 2),
		zap.Uint64("blocks_count", s.Blocks),
		util.ZapDurationWithPrec("write_duration_ms", s.Duration, "ms", 0),
	)
}

type SealingStats []*BlockStats

func (s SealingStats) getOverall() *BlockStats {
	overall := &BlockStats{Name: "overall"}
	for _, blockStats := range s {
		overall.Raw += blockStats.Raw
		overall.Comp += blockStats.Comp
		overall.Blocks += blockStats.Blocks
		overall.Duration += blockStats.Duration
	}
	return overall
}

func (s SealingStats) WriteLogs() {
	for _, blockStats := range s {
		blockStats.WriteLogs()
	}
	s.getOverall().WriteLogs()
}
