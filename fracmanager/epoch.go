package fracmanager

import (
	"time"

	"github.com/ozontech/seq-db/frac"
)

type epochTracker struct {
	enabled      bool
	period       time.Duration
	maxFractions int

	current   uint64
	slot      time.Time
	fractions int
}

func newEpochTracker(cfg *Config, last uint64, now time.Time) epochTracker {
	if !cfg.EpochEnabled {
		return epochTracker{}
	}
	return epochTracker{
		enabled:      true,
		period:       cfg.EpochPeriod,
		maxFractions: cfg.EpochMaxFractions,
		current:      last + 1,
		slot:         now.Truncate(cfg.EpochPeriod),
	}
}

func (e *epochTracker) roll(now time.Time) {
	if !e.enabled {
		return
	}

	slot := now.Truncate(e.period)
	expired := !slot.Equal(e.slot)
	full := e.fractions >= e.maxFractions

	if expired || full {
		e.current++
		e.fractions = 0
		e.slot = slot
	}
}

func (e *epochTracker) assign(now time.Time) uint64 {
	if !e.enabled {
		return 0
	}

	e.roll(now)
	e.fractions++

	return e.current
}

func lastEpoch(
	lowerBound uint64,
	sealed []*frac.Sealed,
	remotes []*frac.Remote,
) uint64 {
	last := lowerBound
	for _, f := range sealed {
		last = max(last, f.Info().Epoch)
	}
	for _, f := range remotes {
		last = max(last, f.Info().Epoch)
	}
	return last
}
