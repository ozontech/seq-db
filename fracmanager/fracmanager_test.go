package fracmanager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	testscommon "github.com/ozontech/seq-db/tests/common"
)

func setupDataDir(t testing.TB, cfg *Config) *Config {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	return cfg
}

func setupFracManager(t testing.TB, cfg *Config) (*Config, *FracManager, func()) {
	cfg = setupDataDir(t, cfg)
	fm, err := New(t.Context(), cfg, nil)
	assert.NoError(t, err)
	fm.Start()
	return cfg, fm, fm.Stop
}

func addDummyDoc(t *testing.T, fm *FracManager, dp *indexer.TestDocProvider, seqID seq.ID) {
	doc := []byte("document")
	dp.Append(doc, nil, seqID, "service:100500", "k8s_pod", "_all_:")
	docs, metas := dp.Provide()
	err := fm.Append(context.Background(), docs, metas)
	assert.NoError(t, err)
}

func MakeSomeFractions(t *testing.T, fm *FracManager) {
	dp := indexer.NewTestDocProvider()
	addDummyDoc(t, fm, dp, seq.SimpleID(1))
	fm.seal(fm.rotate())

	dp.TryReset()

	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.seal(fm.rotate())

	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(3))
}

func TestCleanUp(t *testing.T) {
	cfg, fm, stop := setupFracManager(t, &Config{
		FracSize:  1000,
		TotalSize: 100000,
	})

	MakeSomeFractions(t, fm)

	first := fm.localFracs[0].instance.(*frac.Sealed)
	first.PartialSuicideMode = frac.HalfRename
	first.Suicide()

	second := fm.localFracs[1].instance.(*frac.Sealed)
	second.PartialSuicideMode = frac.HalfRemove
	second.Suicide()
	info := fm.active.frac.Info()
	shouldSealOnExit := info.FullSize() > fm.minFracSizeToSeal()

	stop()

	if shouldSealOnExit && info.DocsTotal > 0 {
		t.Error("active fraction should be empty after rotation and sealing")
	}

	_, fm, stop = setupFracManager(t, cfg)
	defer stop()

	assert.Equal(t, 1, len(fm.localFracs), "wrong frac count")
}

func TestCapacityExceeded(t *testing.T) {
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	launchAndCheck := func(checkFn func(fm *FracManager)) {
		fm, err := New(context.Background(), &Config{
			FracSize:  500,
			TotalSize: 5000,
			DataDir:   dataDir,
		}, nil)
		assert.NoError(t, err)

		checkFn(fm)

		fm.indexer.Stop()
	}

	id := 1
	dp := indexer.NewTestDocProvider()
	makeSealedFrac := func(fm *FracManager, docsPerFrac int) {
		for i := 0; i < docsPerFrac; i++ {
			addDummyDoc(t, fm, dp, seq.SimpleID(int64(id)))
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

		fm.localFracs = nil
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
