package sealer

import (
	"time"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type stats struct {
	info       sectionStats
	tokens     sectionStats
	tokenTable sectionStats
	offsets    sectionStats
	mids       sectionStats
	rids       sectionStats
	pos        sectionStats
	lids       sectionStats
}

type sectionStats struct {
	start       time.Time
	len         int
	rawLen      int
	blocksCount int
}

func startStats() sectionStats {
	return sectionStats{start: time.Now()}
}

func (s *sectionStats) account(block indexBlock) indexBlock {
	s.blocksCount++
	s.len += len(block.payload)
	s.rawLen += block.rawLen
	return block
}

func (s *sectionStats) log(name string, endTime time.Time) {
	ratio := float64(s.rawLen) / float64(s.len)
	logger.Info("seal block stats",
		zap.String("type", name),
		util.ZapUint64AsSizeStr("raw", uint64(s.rawLen)),
		util.ZapUint64AsSizeStr("compressed", uint64(s.len)),
		util.ZapFloat64WithPrec("ratio", ratio, 2),
		zap.Uint64("blocks_count", uint64(s.blocksCount)),
		util.ZapDurationWithPrec("write_duration_ms", endTime.Sub(s.start), "ms", 0),
	)
}
