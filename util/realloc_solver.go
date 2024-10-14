package util

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
)

const (
	defaultReuserThreshold     = 3
	defaultReuserMultiplier    = 2
	defaultReuserStatsPoolSize = 200
)

type ReallocSolver struct {
	iterations uint64
	label      string
	stats      *metric.RollingAverage
	statSize   int
	threshold  float32
	multiplier float32
}

type ReallocSolverOpts func(*ReallocSolver)

func ReallocSolverSize(statSize int) ReallocSolverOpts {
	return func(r *ReallocSolver) { r.statSize = statSize }
}

func ReallocSolverLabel(label string) ReallocSolverOpts {
	return func(r *ReallocSolver) { r.label = label }
}

func NewReallocSolver(opts ...ReallocSolverOpts) ReallocSolver {
	r := ReallocSolver{
		threshold:  defaultReuserThreshold,
		multiplier: defaultReuserMultiplier,
		statSize:   defaultReuserStatsPoolSize,
	}
	for _, opt := range opts {
		opt(&r)
	}
	r.stats = metric.NewRollingAverage(r.statSize)
	return r
}

func (s *ReallocSolver) ReallocParams(curLen, curCap int) (int, bool) {
	s.iterations++
	s.stats.Append(curLen)
	if s.stats.Filled() { // have enough stats
		avg := s.stats.Get()
		topSize := int(avg * s.threshold)
		if topSize < curCap { // allocated to much
			newSize := int(avg * s.multiplier)
			if s.label != "" {
				logger.Debug(
					"need reallocation",
					zap.String("label", s.label),
					zap.Int("cur_len", curLen),
					zap.Int("cur_cap", curCap),
					zap.Int("new_size", newSize),
					zap.Uint64("iterations", s.iterations),
				)
			}
			return newSize, true
		}
	}
	return 0, false
}
