package fracmanager

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/seq"
)

func setupDataDir(t testing.TB, cfg *Config) *Config {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	cfg.Fraction.SkipFsync = true
	return cfg
}

func setupFracManager(t testing.TB, cfg *Config) (*Config, *FracManager, func()) {
	cfg = setupDataDir(t, cfg)
	fm, err := New(t.Context(), cfg, nil)
	assert.NoError(t, err)
	fm.Start()
	return cfg, fm, fm.Stop
}

func addDummyDoc(t *testing.T, fm *FracManager, dp *frac.DocProvider, seqID seq.ID) {
	doc := []byte("document")
	dp.Append(doc, nil, seqID, seq.Tokens("service:100500", "k8s_pod", "_all_:"))
	docs, metas := dp.Provide()
	err := fm.Append(context.Background(), docs, metas)
	assert.NoError(t, err)
}

func MakeSomeFractions(t *testing.T, fm *FracManager) {
	dp := frac.NewDocProvider()
	addDummyDoc(t, fm, dp, seq.SimpleID(1))
	fm.seal(fm.rotate())

	dp.TryReset()

	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.seal(fm.rotate())

	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(3))
}

func TestCleanUp(t *testing.T) {
	cfg := &Config{
		FracSize:  256,
		TotalSize: 10 * uint64(units.KiB),
		Fraction: frac.Config{
			SkipFsync:    true,
			SkipSortDocs: true,
		},
		MaintenanceDelay: 1 * time.Millisecond,
	}

	// first start
	cfg.MinSealFracSize = math.MaxInt64 // to ensure that the frac will not be sealed on shutdown
	cfg, fm, tearDown := setupFracManager(t, cfg)

	MakeSomeFractions(t, fm)

	first := fm.localFracs[0].instance.(*frac.Sealed)
	first.Suicide()

	second := fm.localFracs[1].instance.(*frac.Sealed)
	second.Suicide()

	activeName := fm.Fractions()[2].Info().Name()

	tearDown()

	// second start
	cfg.MinSealFracSize = 1 // to ensure that the frac will be sealed on shutdown
	cfg, fm, tearDown = setupFracManager(t, cfg)

	assert.Equal(t, 1, len(fm.Fractions()), "third fraction should be single")
	assert.Equal(t, activeName, fm.Fractions()[0].Info().Name(), "third fraction should be first now")
	assert.Equal(t, fm.Fractions()[0], fm.Active(), "third fraction should be active")

	tearDown()

	// third start
	_, fm, tearDown = setupFracManager(t, cfg)

	assert.Equal(t, 2, len(fm.Fractions()), "third fraction should be rotated")
	_, ok := fm.Fractions()[0].(*frac.Sealed)
	assert.True(t, ok, "third fraction should be sealed")
	assert.Equal(t, activeName, fm.Fractions()[0].Info().Name(), "third fraction should be rotated")
	assert.Equal(t, uint32(0), fm.Fractions()[1].Info().DocsTotal, "active fraction should be empty")
	assert.Equal(t, fm.Fractions()[1], fm.Active(), "new fraction should be active")

	tearDown()
}

func TestCapacityExceeded(t *testing.T) {
	dataDir := t.TempDir()
	launchAndCheck := func(checkFn func(fm *FracManager)) {
		fm, err := New(context.Background(), &Config{
			FracSize:  500,
			TotalSize: 5000,
			DataDir:   dataDir,
			Fraction:  frac.Config{SkipFsync: true},
		}, nil)
		assert.NoError(t, err)

		checkFn(fm)

		fm.indexer.Stop()
	}

	id := 1
	dp := frac.NewDocProvider()
	makeSealedFrac := func(fm *FracManager, docsPerFrac int) {
		for i := 0; i < docsPerFrac; i++ {
			addDummyDoc(t, fm, dp, seq.SimpleID(id))
			id++
		}
		fm.seal(fm.rotate())
		dp.TryReset()
	}

	// first run
	launchAndCheck(func(fm *FracManager) {
		assert.Equal(t, false, fm.Flags().IsCapacityExceeded(), "expect data dir is empty")
		makeSealedFrac(fm, 10)
		assert.Equal(t, false, fm.Flags().IsCapacityExceeded(), "there should still be no fraction removal and the flag should be false")
	})

	// second run
	launchAndCheck(func(fm *FracManager) {
		assert.Equal(t, false, fm.Flags().IsCapacityExceeded(), "there should still be no fraction removal and the flag should be false")
		for fm.Fractions().GetTotalSize() < fm.config.TotalSize {
			makeSealedFrac(fm, 10)
		}
		assert.Equal(t, false, fm.Flags().IsCapacityExceeded(), "there should still be no fraction removal and the flag should be false")
		sealWG := sync.WaitGroup{}
		suicideWG := sync.WaitGroup{}
		fm.maintenance(&sealWG, &suicideWG)
		assert.Equal(t, true, fm.Flags().IsCapacityExceeded(), "the deletion should occur and the flag should now be true")
	})

	// third run
	launchAndCheck(func(fm *FracManager) {
		assert.Equal(t, true, fm.Flags().IsCapacityExceeded(), "IsCapacityExceeded must be set to true in the state file")
	})

}

func TestOldestCT(t *testing.T) {
	const fracCount = 10

	t.Run("local", func(t *testing.T) {
		fm, err := New(context.Background(), &Config{DataDir: t.TempDir()}, nil)
		assert.NoError(t, err)

		oldestLocal := time.Now()
		nowOldestLocal := oldestLocal

		for i := range fracCount {
			fm.localFracs = append(fm.localFracs, &fracRef{instance: frac.NewSealed(
				"", nil, nil, nil, &common.Info{
					Path:         fmt.Sprintf("local-frac-%d", i),
					IndexOnDisk:  1,
					CreationTime: uint64(nowOldestLocal.UnixMilli()),
				}, nil,
			)})
			nowOldestLocal = nowOldestLocal.Add(time.Second)
		}

		fm.updateOldestCT()

		require.Equal(t, uint64(0), fm.oldestCTRemote.Load())
		require.Equal(t, uint64(oldestLocal.UnixMilli()), fm.oldestCTLocal.Load())
		require.Equal(t, uint64(oldestLocal.UnixMilli()), fm.Oldest())
	})

	t.Run("local-and-remote", func(t *testing.T) {
		fm, err := New(context.Background(), &Config{DataDir: t.TempDir()}, nil)
		assert.NoError(t, err)

		oldestRemote := time.Now()
		nowOldestRemote := oldestRemote

		fm.localFracs = nil

		for i := range fracCount {
			fm.remoteFracs = append(fm.remoteFracs, frac.NewRemote(
				t.Context(), "", nil, nil, nil, &common.Info{
					Path:         fmt.Sprintf("remote-frac-%d", i),
					IndexOnDisk:  1,
					CreationTime: uint64(nowOldestRemote.UnixMilli()),
				}, nil, nil,
			))
			nowOldestRemote = nowOldestRemote.Add(time.Second)
		}

		oldestLocal := nowOldestRemote
		nowOldestLocal := oldestLocal

		for i := range fracCount {
			fm.localFracs = append(fm.localFracs, &fracRef{instance: frac.NewSealed(
				"", nil, nil, nil, &common.Info{
					Path:         fmt.Sprintf("local-frac-%d", i),
					IndexOnDisk:  1,
					CreationTime: uint64(nowOldestLocal.UnixMilli()),
				}, nil,
			)})
			nowOldestLocal = nowOldestLocal.Add(time.Second)
		}

		fm.updateOldestCT()

		require.Equal(t, uint64(oldestRemote.UnixMilli()), fm.oldestCTRemote.Load())
		require.Equal(t, uint64(oldestLocal.UnixMilli()), fm.oldestCTLocal.Load())
		require.Equal(t, uint64(oldestRemote.UnixMilli()), fm.Oldest())
	})
}
