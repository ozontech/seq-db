package fracmanager

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
)

const testEpochPeriod = 100 * 365 * 24 * time.Hour

func testEpochTracker(period time.Duration, maxFractions int, last uint64) epochTracker {
	return newEpochTracker(&Config{
		EpochEnabled:      true,
		EpochPeriod:       period,
		EpochMaxFractions: maxFractions,
	}, last)
}

func TestEpochTracker(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	t.Run("disabled", func(t *testing.T) {
		e := newEpochTracker(&Config{EpochPeriod: 15 * time.Minute, EpochMaxFractions: 2}, 7)
		assert.Equal(t, uint64(0), e.assign(start))
		assert.Equal(t, uint64(0), e.assign(start.Add(time.Hour)))
		e.roll(start.Add(2 * time.Hour))
		assert.Equal(t, uint64(0), e.current)
	})

	t.Run("starts_after_last", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 7)
		assert.Equal(t, uint64(8), e.current)
		assert.Equal(t, uint64(8), e.assign(start))
	})

	t.Run("aligned_period", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0)
		assert.Equal(t, uint64(1), e.assign(start.Add(14*time.Minute)))
		assert.Equal(t, uint64(1), e.assign(start.Add(14*time.Minute+59*time.Second)))
		assert.Equal(t, uint64(2), e.assign(start.Add(15*time.Minute)))
		assert.Equal(t, uint64(2), e.assign(start.Add(29*time.Minute)))
		assert.Equal(t, uint64(3), e.assign(start.Add(2*time.Hour)))
	})

	t.Run("max_fractions", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 2, 0)
		assert.Equal(t, uint64(1), e.assign(start))
		assert.Equal(t, uint64(1), e.assign(start))
		assert.Equal(t, uint64(1), e.current, "full epoch stays current until the next roll")
		assert.Equal(t, uint64(2), e.assign(start))
		assert.Equal(t, uint64(2), e.assign(start))
		assert.Equal(t, uint64(3), e.assign(start))
	})

	t.Run("roll_closes_expired", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0)
		assert.Equal(t, uint64(1), e.assign(start))

		e.roll(start.Add(14 * time.Minute))
		assert.Equal(t, uint64(1), e.current)

		e.roll(start.Add(15 * time.Minute))
		assert.Equal(t, uint64(2), e.current)
	})

	t.Run("roll_closes_full", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 1, 0)
		assert.Equal(t, uint64(1), e.assign(start))

		e.roll(start)
		assert.Equal(t, uint64(2), e.current)
	})

	t.Run("empty_epoch_never_closes", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 1, 0)

		e.roll(start.Add(time.Hour))
		e.roll(start.Add(2 * time.Hour))
		assert.Equal(t, uint64(1), e.current)

		assert.Equal(t, uint64(1), e.assign(start.Add(3*time.Hour)))
		e.roll(start.Add(3*time.Hour + time.Minute))
		e.roll(start.Add(4 * time.Hour))
		assert.Equal(t, uint64(2), e.current)
	})

	t.Run("clock_moved_back", func(t *testing.T) {
		e := testEpochTracker(15*time.Minute, 100, 0)
		assert.Equal(t, uint64(1), e.assign(start))
		assert.Equal(t, uint64(2), e.assign(start.Add(-time.Hour)))
		assert.Equal(t, uint64(3), e.assign(start))
	})
}

func fillAndRotate(t *testing.T, lc *lifecycleManager) {
	appendDocsToActive(t, lc.registry.appender().Active, 10)

	wg := sync.WaitGroup{}
	lc.rotate(0, &wg)
	wg.Wait()
}

func sealedEpochs(reg *fractionRegistry) []uint64 {
	fracs, releases := reg.acquireFractionsFunc(func(f frac.Fraction) bool {
		_, ok := f.(*refCountedSealed)
		return ok
	})

	epochs := make([]uint64, 0, len(fracs))
	for i, f := range fracs {
		epochs = append(epochs, f.Info().Epoch)
		releases[i]()
	}

	slices.Sort(epochs)
	return epochs
}

func TestEpochAssignedOnRotate(t *testing.T) {
	lc, tearDown := setupLifecycle(t, &Config{
		Fraction:          frac.Config{SkipSortDocs: true},
		EpochEnabled:      true,
		EpochPeriod:       testEpochPeriod,
		EpochMaxFractions: 2,
		EpochLowerBound:   4,
	})
	defer tearDown()

	for range 5 {
		fillAndRotate(t, lc)
	}

	assert.Equal(t, []uint64{5, 5, 6, 6, 7}, sealedEpochs(lc.registry))
	assert.Equal(t, uint64(0), lc.registry.appender().Info().Epoch, "active fraction has no epoch until rotated")

	var onDisk []uint64
	for _, s := range lc.registry.sealedSnapshot() {
		onDisk = append(onDisk, lc.provider.NewSealed(s.BaseFileName, nil).Info().Epoch)
	}
	slices.Sort(onDisk)
	assert.Equal(t, []uint64{5, 5, 6, 6, 7}, onDisk, "epoch must be persisted in the info file")
}

func TestEpochDisabled(t *testing.T) {
	lc, tearDown := setupLifecycle(t, &Config{
		Fraction:          frac.Config{SkipSortDocs: true},
		EpochPeriod:       testEpochPeriod,
		EpochMaxFractions: 1,
		EpochLowerBound:   4,
	})
	defer tearDown()

	for range 3 {
		fillAndRotate(t, lc)
	}

	assert.Equal(t, []uint64{0, 0, 0}, sealedEpochs(lc.registry))
	assert.Equal(t, uint64(0), lc.registry.rollEpoch(time.Now()))
	assert.Equal(t, uint64(0), lc.registry.oldestOpenEpoch())
}

func TestOldestOpenEpochWithSealingInFlight(t *testing.T) {
	lc, tearDown := setupLifecycle(t, &Config{
		Fraction:          frac.Config{SkipSortDocs: true},
		EpochEnabled:      true,
		EpochPeriod:       testEpochPeriod,
		EpochMaxFractions: 1,
	})
	defer tearDown()

	assert.Equal(t, uint64(1), lc.registry.oldestOpenEpoch(), "nothing is closed yet")

	fillAndRotate(t, lc)
	assert.Equal(t, uint64(1), lc.registry.oldestOpenEpoch(), "full epoch is open until rolled")
	assert.Equal(t, uint64(2), lc.registry.rollEpoch(time.Now()))
	assert.Equal(t, uint64(2), lc.registry.oldestOpenEpoch())

	appendDocsToActive(t, lc.registry.appender().Active, 10)
	inFlight, waitBeforeSealing, err := lc.registry.rotateIfFull(0, lc.provider)
	require.NoError(t, err)
	require.NotNil(t, inFlight)
	waitBeforeSealing()
	assert.Equal(t, uint64(2), inFlight.Info().Epoch)

	fillAndRotate(t, lc)
	fillAndRotate(t, lc)
	assert.Equal(t, uint64(5), lc.registry.rollEpoch(time.Now()))
	assert.Equal(t, []uint64{1, 3, 4}, sealedEpochs(lc.registry))
	assert.Equal(t, uint64(2), lc.registry.oldestOpenEpoch(), "epoch with a sealing fraction is not closed")

	sealed, err := lc.provider.Seal(inFlight.Active)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), lc.registry.oldestOpenEpoch(), "epoch is not closed until its fraction is promoted")

	lc.registry.promoteToSealed(inFlight, sealed)
	inFlight.Destroy()

	assert.Equal(t, []uint64{1, 2, 3, 4}, sealedEpochs(lc.registry))
	assert.Equal(t, uint64(5), lc.registry.oldestOpenEpoch())
}

func TestAcquireFractionsFunc(t *testing.T) {
	lc, tearDown := setupLifecycle(t, &Config{
		Fraction:          frac.Config{SkipSortDocs: true},
		EpochEnabled:      true,
		EpochPeriod:       testEpochPeriod,
		EpochMaxFractions: 2,
	})
	defer tearDown()

	for range 5 {
		fillAndRotate(t, lc)
	}

	fracs, releases := lc.registry.acquireFractionsFunc(func(f frac.Fraction) bool {
		return f.Info().Epoch == 1
	})
	require.Len(t, fracs, 2)
	require.Len(t, releases, 2)

	wg := sync.WaitGroup{}
	lc.cleanLocal(lc.registry.appender().Info().FullSize(), &wg)

	destroyed := make(chan struct{})
	go func() {
		wg.Wait()
		close(destroyed)
	}()

	releases[0]()
	select {
	case <-destroyed:
		t.Fatal("fraction is destroyed while it is still acquired")
	case <-time.After(50 * time.Millisecond):
	}

	for _, f := range fracs {
		assert.Equal(t, uint64(1), f.Info().Epoch)
	}

	releases[1]()
	<-destroyed

	fracs, releases = lc.registry.acquireFractionsFunc(func(frac.Fraction) bool { return true })
	assert.Len(t, fracs, 1, "only the active fraction is left")
	assert.Len(t, releases, 1)
	releases[0]()
}

func TestEpochMonotonicAfterRestart(t *testing.T) {
	cfg := &Config{
		FracSize:          1 * uint64(units.MiB),
		TotalSize:         1 * uint64(units.MiB),
		Fraction:          frac.Config{SkipSortDocs: true},
		EpochEnabled:      true,
		EpochPeriod:       testEpochPeriod,
		EpochMaxFractions: 1,
	}

	cfg, fm, stop := setupFracManager(t, cfg)
	for range 2 {
		appendDocsToFracManager(t, fm, 10)
		fm.SealForcedForTests()
	}
	assert.Equal(t, []uint64{1, 2}, sealedEpochs(fm.lc.registry))
	appendDocsToFracManager(t, fm, 10)
	stop()

	cfg, fm, stop = setupFracManager(t, cfg)
	assert.Equal(t, []uint64{1, 2}, sealedEpochs(fm.lc.registry))
	assert.Equal(t, uint64(3), fm.OldestOpenEpoch(), "epochs found on disk are closed")
	fm.SealForcedForTests()
	assert.Equal(t, []uint64{1, 2, 3}, sealedEpochs(fm.lc.registry), "replayed active fraction gets a new epoch")
	stop()

	cfg.MinSealFracSize = 1
	cfg, fm, stop = setupFracManager(t, cfg)
	assert.Equal(t, uint64(4), fm.OldestOpenEpoch())
	appendDocsToFracManager(t, fm, 10)
	stop()

	cfg.MinSealFracSize = 0
	cfg, fm, stop = setupFracManager(t, cfg)
	assert.Equal(t, []uint64{1, 2, 3, 4}, sealedEpochs(fm.lc.registry), "fraction sealed on shutdown gets an epoch")
	assert.Equal(t, uint64(5), fm.OldestOpenEpoch())
	stop()

	cfg.EpochLowerBound = 10
	_, fm, stop = setupFracManager(t, cfg)
	assert.Equal(t, uint64(11), fm.OldestOpenEpoch(), "lower bound wins over info files")
	appendDocsToFracManager(t, fm, 10)
	fm.SealForcedForTests()
	assert.Equal(t, []uint64{1, 2, 3, 4, 11}, sealedEpochs(fm.lc.registry))
	stop()
}

func TestReplayAndSealAssignsEpoch(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	actives := make([]*frac.Active, 0, 4)
	for range 3 {
		active := fp.CreateActive()
		appendDocsToActive(t, active, 100)
		actives = append(actives, active)
	}
	last := fp.CreateActive()
	appendDocsToActive(t, last, 5)
	actives = append(actives, last)

	epochs := testEpochTracker(testEpochPeriod, 2, 7)
	active, sealed, err := loader.replayAndSeal(t.Context(), actives, &epochs)
	require.NoError(t, err)
	require.Len(t, sealed, 3)

	assert.Equal(t, uint64(0), active.Info().Epoch, "fraction that stays active has no epoch")

	want := []uint64{8, 8, 9}
	for i, s := range sealed {
		assert.Equal(t, want[i], s.Info().Epoch)
		assert.Equal(t, want[i], fp.NewSealed(s.BaseFileName, nil).Info().Epoch, "epoch must be persisted in the info file")
	}
	assert.Equal(t, uint64(9), epochs.current)
}
