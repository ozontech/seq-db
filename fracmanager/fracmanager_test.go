package fracmanager

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
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
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:     1000,
		TotalSize:    100000,
		ShouldReplay: false,
		DataDir:      dataDir,
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
		FracSize:     100,
		TotalSize:    100000,
		ShouldReplay: false,
		DataDir:      dataDir,
	})

	assert.NoError(t, err)

	defer fm.Stop()

	assert.Equal(t, 1, len(fm.localFracs), "wrong frac count")
}

func TestReplayFractions(t *testing.T) {
	fracCount := 10
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	fm, err := newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:     100000000,
		TotalSize:    100000000,
		ShouldReplay: false,
		DataDir:      dataDir,
	})
	assert.NoError(t, err)

	for i := 0; i < fracCount; i++ {
		addDocs(t, fm, 500+rand.Intn(100))
		fm.rotate()
	}
	// active frac is empty

	var originalFracs []frac.Info
	for _, frac := range fm.getLocalFracs() {
		originalFracs = append(originalFracs, *frac.Info())
	}

	assert.Equal(t, fracCount+1, len(originalFracs))

	active := fm.Active()
	assert.Equal(t, active.Info().DocsTotal, uint32(0), "active fraction should have no documents")

	fm.Stop()

	fm, err = newFracManagerWithBackgroundStart(t.Context(), &Config{
		FracSize:     100000000,
		TotalSize:    100000000,
		ShouldReplay: true,
		DataDir:      dataDir,
	})
	assert.NoError(t, err)

	replayedFracs := fm.getLocalFracs()

	assert.Equal(t, len(originalFracs), len(replayedFracs), "should replay same number of fractions")

	for i := 0; i < 10; i++ {
		originalFracInfo := originalFracs[i]
		replayedFracInfo := replayedFracs[i].Info()

		assert.Equal(t, originalFracInfo.Name(), replayedFracInfo.Name(), "fraction %d should have same name", i)
		assert.Equal(t, originalFracInfo.DocsTotal, replayedFracInfo.DocsTotal, "fraction %d should have same doc count", i)
	}

	assert.Equal(t, uint32(0), replayedFracs[10].Info().DocsTotal, "last fraction should have no documents")

	newActive := fm.Active()
	assert.Equal(t, uint64(0), newActive.Info().DocsOnDisk, "new active fraction should be empty")

	fm.Stop()
}

func addDocs(t *testing.T, fm *FracManager, docCount int) {
	dp := frac.NewDocProvider()
	for i := 0; i < docCount; i++ {
		doc := []byte("{\"timestamp\": 0, \"message\": \"msg\"}")
		docRoot, err := insaneJSON.DecodeBytes(doc)
		assert.NoError(t, err)
		dp.Append(doc, docRoot, seq.SimpleID(i), seq.Tokens("service:100500", "k8s_pod", "_all_:"))
	}

	docs, metas := dp.Provide()
	err := fm.Append(context.Background(), docs, metas)
	assert.NoError(t, err)
}

func TestMatureMode(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	launchAndCheck := func(checkFn func(fm *FracManager)) {
		fm := NewFracManager(context.Background(), &Config{
			FracSize:     500,
			TotalSize:    5000,
			ShouldReplay: false,
			DataDir:      dataDir,
		}, nil)
		assert.NoError(t, fm.Load(context.Background()))

		checkFn(fm)

		fm.fracProvider.Stop()
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

func TestNewULID(t *testing.T) {
	fm := NewFracManager(context.Background(), &Config{}, nil)
	ulid1 := fm.nextFractionID()
	ulid2 := fm.nextFractionID()
	assert.NotEqual(t, ulid1, ulid2, "ULIDs should be different")
	assert.Equal(t, 26, len(ulid1), "ULID should have length 26")
	assert.Greater(t, ulid2, ulid1)
}
