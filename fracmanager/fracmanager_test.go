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

func TestOldestCT(t *testing.T) {
	const fracCount = 10

	t.Run("local", func(t *testing.T) {
		fm := NewFracManager(context.Background(), &Config{}, nil)

		oldestLocal := time.Now()
		nowOldestLocal := oldestLocal

		for i := range fracCount {
			fm.localFracs = append(fm.localFracs, &fracRef{instance: frac.NewSealed(
				"", nil, nil, nil, &frac.Info{
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
				t.Context(), "", nil, nil, nil, &frac.Info{
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
				"", nil, nil, nil, &frac.Info{
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
