package fracmanager

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

type testFakeFrac struct {
	frac.Fraction
	counter atomic.Int64
}

func (t *testFakeFrac) IsIntersecting(_, _ seq.MID) bool {
	return true
}

func TestProvideLimit(t *testing.T) {
	maxFractionHits := 10
	fracsCount := maxFractionHits + 10
	testFracs := make([]*fracRef, 0, fracsCount)
	for i := 0; i < fracsCount; i++ {
		testFracs = append(testFracs, &fracRef{instance: &testFakeFrac{}})
	}

	cfg := FillConfigWithDefault(&Config{MaxFractionHits: uint64(maxFractionHits)})
	fm := &FracManager{config: cfg, fracs: testFracs}
	_, err := fm.SelectFracsInRange(0, 0)
	assert.Error(t, err)
	for _, f := range testFracs {
		v := f.instance.(*testFakeFrac)
		assert.Equal(t, v.counter.Load(), int64(0))
	}
}

func addDummyDoc(t *testing.T, fm *FracManager, dp *frac.DocProvider, seqID seq.ID) {
	doc := []byte("document")
	dp.Append(doc, nil, seqID, query.Tokens("service:100500", "k8s_pod"))
	docs, metas := dp.Provide()
	err := fm.Append(context.Background(), docs, metas, atomic.NewUint64(0))
	assert.NoError(t, err)
}

func MakeSomeFractions(t *testing.T, fm *FracManager) {
	dp := frac.NewDocProvider(query.TestMapping, consts.DefaultMaxTokenSize, false)
	addDummyDoc(t, fm, dp, seq.SimpleID(1))
	fm.GetActiveFrac().WaitWriteIdle()
	fm.seal(fm.rotate())

	dp.TryReset()

	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.GetActiveFrac().WaitWriteIdle()
	fm.seal(fm.rotate())

	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(3))
	fm.GetActiveFrac().WaitWriteIdle()
}

func TestCleanUp(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	fm, err := NewFracManagerWithBackgroundStart(&Config{
		FracSize:         1000,
		TotalSize:        100000,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})

	assert.NoError(t, err)

	MakeSomeFractions(t, fm)

	first := fm.fracs[0].instance.(*frac.Sealed)
	first.PartialSuicideMode = frac.HalfRename
	first.Suicide()

	second := fm.fracs[1].instance.(*frac.Sealed)
	second.PartialSuicideMode = frac.HalfRemove
	second.Suicide()

	shouldSealOnExit := fm.shouldSealOnExit(fm.active.frac)
	fm.Stop()
	if shouldSealOnExit && fm.active.frac.Info().DocsTotal > 0 {
		t.Error("active fraction should be empty after rotation and sealing")
	}

	fm, err = NewFracManagerWithBackgroundStart(&Config{
		FracSize:         100,
		TotalSize:        100000,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})

	assert.NoError(t, err)

	defer fm.Stop()

	assert.Equal(t, 1, len(fm.fracs), "wrong frac count")
}

func TestMatureMode(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	launchAndCheck := func(checkFn func(fm *FracManager)) {
		fm := NewFracManager(&Config{
			FracSize:         500,
			TotalSize:        5000,
			ShouldReplay:     false,
			ShouldRemoveMeta: true,
			DataDir:          dataDir,
		})
		assert.NoError(t, fm.Load(context.Background()))

		checkFn(fm)

		fm.indexWorkers.Stop()
		fm.reader.Stop()
	}

	id := 1
	dp := frac.NewDocProvider(query.TestMapping, consts.DefaultMaxTokenSize, false)
	makeSealedFrac := func(fm *FracManager, docsPerFrac int) {
		for i := 0; i < docsPerFrac; i++ {
			addDummyDoc(t, fm, dp, seq.SimpleID(id))
			id++
		}
		fm.GetActiveFrac().WaitWriteIdle()
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
		for fm.GetAllFracs().getTotalSize() < fm.config.TotalSize {
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
	fm := NewFracManager(&Config{})
	ulid1 := fm.nextFractionID()
	ulid2 := fm.nextFractionID()
	assert.NotEqual(t, ulid1, ulid2, "ULIDs should be different")
	assert.Equal(t, 26, len(ulid1), "ULID should have length 26")
	assert.Greater(t, ulid2, ulid1)
}
