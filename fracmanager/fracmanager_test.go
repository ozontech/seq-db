package fracmanager

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	testscommon "github.com/ozontech/seq-db/tests/common"
)

// newFracManagerWithBackgroundStart only used from tests
func newFracManagerWithBackgroundStart(ctx context.Context, config *Config) (*FracManager, error) {
	fracManager := NewFracManager(ctx, config, nil)
	if err := fracManager.Load(ctx); err != nil {
		return nil, err
	}
	fracManager.Start()
	return fracManager, nil
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
	dataDir := testscommon.GetTestTmpDir(t)

	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:  1000,
		TotalSize: 100000,
		DataDir:   dataDir,
	})

	assert.NoError(t, err)

	MakeSomeFractions(t, fm)

	first := fm.localFracs[0].instance.(*frac.Sealed)
	first.PartialSuicideMode = frac.HalfRename
	first.Suicide()

	second := fm.localFracs[1].instance.(*frac.Sealed)
	second.PartialSuicideMode = frac.HalfRemove
	second.Suicide()
	info := fm.active.frac.Info()
	shouldSealOnExit := info.FullSize() > fm.minFracSizeToSeal()

	fm.Stop()
	if shouldSealOnExit && info.DocsTotal > 0 {
		t.Error("active fraction should be empty after rotation and sealing")
	}

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:  100,
		TotalSize: 100000,
		DataDir:   dataDir,
	})

	assert.NoError(t, err)

	defer fm.Stop()

	assert.Equal(t, 1, len(fm.localFracs), "wrong frac count")
}

func TestReplaySingleEmptyFrac(t *testing.T) {
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:  100000000, // maintenance will not seal fracs
		TotalSize: 100000000,
		DataDir:   dataDir,
	})
	assert.NoError(t, err)

	fractionInfo := fm.localFracs[0].instance.Info()

	fm.Stop()

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:  100000000,
		TotalSize: 100000000,
		DataDir:   dataDir,
	})
	assert.NoError(t, err)

	replayedFracs := fm.getLocalFracs()
	assert.Equal(t, 1, len(replayedFracs), "should replay exactly one frac")
	active := fm.Active() // replayed frac is active
	assert.Equal(t, uint32(0), active.Info().DocsTotal, "no docs")
	assert.NotEqual(t, fractionInfo.Name(), active.Info().Name(), "should create a new empty frac")

	fm.Stop()
}

func TestReplayContextCancel(t *testing.T) {
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000, // maintenance will not seal fracs
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		addDocs(t, fm, 1000+rand.Intn(100))
		fm.rotate()
	}

	fm.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Microsecond)
	defer cancel()

	fm, err = newFracManagerWithBackgroundStart(ctx, &Config{
		FracSize:      100000000,
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

func TestReplaySingleNonEmptyFrac(t *testing.T) {
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000, // maintenance will not seal fracs
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	addDocs(t, fm, 500+rand.Intn(100))
	fractionInfo := fm.localFracs[0].instance.Info()

	assert.Equal(t, 1, len(fm.localFracs), "should have exactly one frac")

	fm.Stop()

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000,
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	replayedFracs := fm.getLocalFracs()
	assert.Equal(t, 1, len(replayedFracs), "should replay exactly one frac")
	active := fm.Active() // replayed frac is active
	assert.Equal(t, fractionInfo.DocsTotal, active.Info().DocsTotal, "should have same doc count for replayed frac")

	fm.Stop()
}

func TestReplayMultipleFracs(t *testing.T) {
	fracCount := 50
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000, // maintenance will not seal fracs
		TotalSize:     100000000,
		ReplayWorkers: 8,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	for i := 0; i < fracCount; i++ {
		addDocs(t, fm, 500+rand.Intn(100))
		fm.rotate()
	}
	addDocs(t, fm, 5)

	var fracs []common.Info
	for _, fraction := range fm.getLocalFracs() {
		info := *fraction.Info()
		fracs = append(fracs, info)
	}

	fm.Stop()

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000,
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	replayedFracs := fm.getLocalFracs()

	assert.Equal(t, len(fracs), len(replayedFracs), "should replay same number of fractions")

	// all fracs should match exactly (no empty) in same order
	for i := 0; i < fracCount+1; i++ {
		assert.Equal(t, fracs[i].Name(), replayedFracs[i].Info().Name(), "fraction %d should have same name", i)
		assert.Equal(t, fracs[i].DocsTotal, replayedFracs[i].Info().DocsTotal, "fraction %d should have same doc count", i)

		if i != fracCount {
			assert.Greater(t, replayedFracs[i].Info().SealingTime, uint64(0), "replayed frac %d must be sealed", i)
		} else {
			assert.Equal(t, replayedFracs[i].Info().SealingTime, uint64(0), "replayed frac %d must not be sealed", i)
		}
	}

	newActive := fm.Active()
	assert.Equal(t, newActive.Info().DocsTotal, uint32(5), "new active fraction should not be empty")

	fm.Stop()
}

func TestReplayFracsWithEmptyActiveFrac(t *testing.T) {
	fracCount := 50
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000, // maintenance will not seal fracs
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	for i := 0; i < fracCount; i++ {
		addDocs(t, fm, 500+rand.Intn(100))
		fm.rotate()
	}
	// active frac is now empty

	var fracs []common.Info
	for _, fraction := range fm.getLocalFracs() {
		fracs = append(fracs, *fraction.Info())
	}

	fm.Stop()

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000,
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	replayedFracs := fm.getLocalFracs()

	assert.Equal(t, len(fracs), len(replayedFracs), "should replay same number of fractions")

	for i := 0; i < fracCount; i++ {
		assert.Equal(t, fracs[i].Name(), replayedFracs[i].Info().Name(), "fraction %d should have same name", i)
		assert.Equal(t, fracs[i].DocsTotal, replayedFracs[i].Info().DocsTotal, "fraction %d should have same doc count", i)
		assert.Greater(t, replayedFracs[i].Info().SealingTime, uint64(0), "replayed frac %d must be sealed", i)
	}

	assert.NotEqual(t, fracs[fracCount].Name(), replayedFracs[fracCount].Info().Name(), "should create a new empty frac")
	assert.Equal(t, uint32(0), replayedFracs[fracCount].Info().DocsTotal, "last fraction should have no documents")

	newActive := fm.Active()
	assert.Equal(t, uint32(0), newActive.Info().DocsTotal, "new active fraction should be empty")

	fm.Stop()
}

func TestReplayFractionsWithMultipleEmptyFracs(t *testing.T) {
	fracCount := 10
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000, // maintenance will not seal fracs
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})
	assert.NoError(t, err)

	for i := 0; i < fracCount; i++ {
		if i%3 == 0 {
			addDocs(t, fm, 500+rand.Intn(100))
		}
		fm.rotate()
	}

	var nonEmptyFracs []common.Info
	for _, fraction := range fm.getLocalFracs() {
		if fraction.Info().DocsTotal > 0 {
			nonEmptyFracs = append(nonEmptyFracs, *fraction.Info())
		}
	}

	assert.Equal(t, 4, len(nonEmptyFracs), "non empty frac count doesn't match")

	fm.Stop()

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:      100000000,
		TotalSize:     100000000,
		ReplayWorkers: 10,
		DataDir:       dataDir,
	})

	assert.NoError(t, err)

	replayedFracs := fm.getLocalFracs()

	assert.Equal(t, len(nonEmptyFracs)+1, len(replayedFracs), "only non-empty fracs and one active empty frac should remain")

	for i := 0; i < 4; i++ {
		assert.Equal(t, nonEmptyFracs[i].Name(), replayedFracs[i].Info().Name(), "fraction %d should have same name", i)
		assert.Equal(t, nonEmptyFracs[i].DocsTotal, replayedFracs[i].Info().DocsTotal, "fraction %d should have same doc count", i)
		assert.Greater(t, replayedFracs[i].Info().SealingTime, uint64(0), "replayed frac %d must be sealed", i)
	}
	assert.Equal(t, uint32(0), fm.Active().Info().DocsTotal, "new active fraction should be empty")

	fm.Stop()
}

func addDocs(t *testing.T, fm *FracManager, docCount int) {
	dp := indexer.NewTestDocProvider()
	for i := 0; i < docCount; i++ {
		doc := []byte("{\"timestamp\": 0, \"message\": \"msg\"}")
		docRoot, err := insaneJSON.DecodeBytes(doc)
		assert.NoError(t, err)
		dp.Append(doc, docRoot, seq.SimpleID(i), "service:100500", "k8s_pod", "_all_:")
	}

	docs, metas := dp.Provide()
	err := fm.Append(context.Background(), docs, metas)
	assert.NoError(t, err)
	fm.WaitIdle()
}

func TestMatureMode(t *testing.T) {
	dataDir := testscommon.GetTestTmpDir(t)
	testscommon.RecreateDir(dataDir)
	defer testscommon.RemoveDir(dataDir)

	launchAndCheck := func(checkFn func(fm *FracManager)) {
		fm := NewFracManager(context.Background(), &Config{
			FracSize:  500,
			TotalSize: 5000,
			DataDir:   dataDir,
		}, nil)
		assert.NoError(t, fm.Load(context.Background()))

		checkFn(fm)

		fm.indexer.Stop()
	}

	id := 1
	dp := indexer.NewTestDocProvider()
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
		assert.Equal(t, false, fm.Mature(), "expect data dir is empty")
		makeSealedFrac(fm, 10)
		assert.Equal(t, false, fm.Mature(), "file .immature must still exist")
	})

	// second run
	launchAndCheck(func(fm *FracManager) {
		assert.Equal(t, false, fm.Mature(), "file .immature must exist")
		for fm.GetAllFracs().GetTotalSize() < fm.config.TotalSize {
			makeSealedFrac(fm, 10)
		}
		assert.Equal(t, false, fm.Mature(), "file .immature must still exist")
		sealWG := sync.WaitGroup{}
		suicideWG := sync.WaitGroup{}
		fm.maintenance(&sealWG, &suicideWG)
		assert.Equal(t, true, fm.Mature(), "file .immature have to be removed")
	})

	// third run
	launchAndCheck(func(fm *FracManager) {
		assert.Equal(t, true, fm.Mature(), "the data directory is not empty at startup and the .immature file must be missing")
	})

}

func TestOldestCT(t *testing.T) {
	const fracCount = 10

	t.Run("local", func(t *testing.T) {
		fm := NewFracManager(context.Background(), &Config{}, nil)

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
		require.Equal(t, uint64(oldestLocal.UnixMilli()), fm.OldestCT())
	})

	t.Run("local-and-remote", func(t *testing.T) {
		fm := NewFracManager(context.Background(), &Config{}, nil)
		oldestRemote := time.Now()
		nowOldestRemote := oldestRemote

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
		require.Equal(t, uint64(oldestRemote.UnixMilli()), fm.OldestCT())
	})
}
