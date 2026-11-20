package fracmanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testEpochTracker(
	period time.Duration,
	maxFractions int,
	last uint64,
	now time.Time,
) epochTracker {
	return newEpochTracker(&Config{
		EpochEnabled:      true,
		EpochPeriod:       period,
		EpochMaxFractions: maxFractions,
	}, last, now)
}

func TestEpochTracker(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	t.Run("disabled", func(t *testing.T) {
		e := newEpochTracker(&Config{EpochPeriod: 15 * time.Minute, EpochMaxFractions: 2}, 7, start)
		assert.Equal(t, uint64(0), e.assign(start))
		assert.Equal(t, uint64(0), e.assign(start.Add(time.Hour)))
		e.roll(start.Add(2 * time.Hour))
		assert.Equal(t, uint64(0), e.current)
	})

	t.Run("starts_after_last", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 7, start)
		assert.Equal(t, uint64(8), e.current)
		assert.Equal(t, uint64(8), e.assign(start))
	})

	t.Run("aligned_period", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0, start)
		assert.Equal(t, uint64(1), e.assign(start.Add(14*time.Minute)))
		assert.Equal(t, uint64(1), e.assign(start.Add(14*time.Minute+59*time.Second)))
		assert.Equal(t, uint64(2), e.assign(start.Add(15*time.Minute)))
		assert.Equal(t, uint64(2), e.assign(start.Add(29*time.Minute)))
		assert.Equal(t, uint64(3), e.assign(start.Add(2*time.Hour)))
	})

	t.Run("max_fractions", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 2, 0, start)
		assert.Equal(t, uint64(1), e.assign(start))
		assert.Equal(t, uint64(1), e.assign(start))
		assert.Equal(t, uint64(1), e.current, "full epoch stays current until the next roll")
		assert.Equal(t, uint64(2), e.assign(start))
		assert.Equal(t, uint64(2), e.assign(start))
		assert.Equal(t, uint64(3), e.assign(start))
	})

	t.Run("roll_closes_expired", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0, start)
		assert.Equal(t, uint64(1), e.assign(start))

		e.roll(start.Add(14 * time.Minute))
		assert.Equal(t, uint64(1), e.current)

		e.roll(start.Add(15 * time.Minute))
		assert.Equal(t, uint64(2), e.current)
	})

	t.Run("roll_closes_full", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 1, 0, start)
		assert.Equal(t, uint64(1), e.assign(start))

		e.roll(start)
		assert.Equal(t, uint64(2), e.current)
	})

	t.Run("roll_closes_empty", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0, start)

		e.roll(start.Add(14 * time.Minute))
		assert.Equal(t, uint64(1), e.current)

		e.roll(start.Add(time.Hour))
		e.roll(start.Add(2 * time.Hour))
		assert.Equal(t, uint64(3), e.current)

		assert.Equal(t, uint64(4), e.assign(start.Add(3*time.Hour)))
		e.roll(start.Add(3*time.Hour + time.Minute))
		assert.Equal(t, uint64(4), e.current)
	})

	t.Run("clock_moved_back", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0, start)
		assert.Equal(t, uint64(1), e.assign(start))
		assert.Equal(t, uint64(2), e.assign(start.Add(-time.Hour)))
		assert.Equal(t, uint64(3), e.assign(start))
	})
}
