package fracmanager

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

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
	fm.SealForcedForTests()

	dp.TryReset()

	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.SealForcedForTests()

	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(3))
}

func TestCleanUp(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	fm, stop, err := New(t.Context(), &Config{
		FracSize:  1000,
		TotalSize: 100000,
		DataDir:   dataDir,
	}, nil)

	assert.NoError(t, err)

	MakeSomeFractions(t, fm)

	first := fm.lm.registry.locals[0].instance
	first.PartialSuicideMode = frac.HalfRename
	first.Suicide()

	second := fm.lm.registry.locals[1].instance
	second.PartialSuicideMode = frac.HalfRemove
	second.Suicide()

	stop()

	fm, stop, err = New(t.Context(), &Config{
		FracSize:  100,
		TotalSize: 100000,
		DataDir:   dataDir,
	}, nil)
	defer stop()

	assert.NoError(t, err)

	assert.Equal(t, 1, len(fm.Fractions()), "we suicided 2 sealed fractions and only one active must leave")
	assert.Greater(t, fm.Fractions()[0].Info().DocsTotal, uint32(0), "active fractions must be non empty")
}

func TestCapacityExceededMode(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	launchAndCheck := func(checkFn func(*FracManager, Config)) {
		cfg := Config{
			FracSize:  500,
			TotalSize: 5000,
			DataDir:   dataDir,
		}
		fm, stop, err := New(context.Background(), &cfg, nil)
		assert.NoError(t, err)
		checkFn(fm, cfg)
		stop()
	}

	id := 1
	dp := frac.NewDocProvider()
	makeSealedFrac := func(fm *FracManager, docsPerFrac int) {
		for i := 0; i < docsPerFrac; i++ {
			addDummyDoc(t, fm, dp, seq.SimpleID(id))
			id++
		}
		fm.SealForcedForTests()
		dp.TryReset()
	}

	// first run
	launchAndCheck(func(fm *FracManager, _ Config) {
		assert.Equal(t, false, fm.IsCapacityExceeded(), "expect data dir is empty")
		makeSealedFrac(fm, 10)
		assert.Equal(t, false, fm.IsCapacityExceeded(), "file .immature must still exist")
	})

	// second run
	launchAndCheck(func(fm *FracManager, cfg Config) {
		assert.Equal(t, false, fm.IsCapacityExceeded(), "file .immature must exist")
		for fm.Fractions().GetTotalSize() < cfg.TotalSize {
			makeSealedFrac(fm, 10)
		}
		assert.Equal(t, false, fm.IsCapacityExceeded(), "file .immature must still exist")
		wg := sync.WaitGroup{}
		fm.lm.Maintain(context.Background(), &wg)
		assert.Equal(t, true, fm.IsCapacityExceeded(), "file .immature have to be removed")
	})

	// third run
	launchAndCheck(func(fm *FracManager, _ Config) {
		assert.Equal(t, true, fm.IsCapacityExceeded(), "the data directory is not empty at startup and the .immature file must be missing")
	})
}
