package compaction

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/alecthomas/units"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

func TestPlannerUsesReplacement(t *testing.T) {
	fm, sealParams, stopManager := setupPlannerManager(t)
	defer stopManager()

	p := newTestPlanner(t, fm)
	sources := plannerSourceNames(fm)
	require.Len(t, sources, 2)

	executor := NewExecutor(1, sealParams, p)
	defer executor.Stop()

	task, ok := p.pick()
	require.True(t, ok)
	defer task.replacement.Release()

	planData, err := os.ReadFile(task.filename + consts.ReplacementPlan)
	require.NoError(t, err)
	var plan struct {
		Participants []string `json:"participants"`
	}
	require.NoError(t, json.Unmarshal(planData, &plan))
	require.ElementsMatch(t, sources, plan.Participants)

	done := make(chan struct{})
	onComplete := task.onComplete
	task.onComplete = func(data *sealed.PreloadedData, err error) {
		defer close(done)
		onComplete(data, err)
	}
	p.tasks <- task

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "compaction did not finish")
	}

	producedName := filepath.Base(task.filename)
	produced, release, ok := fm.AcquireFraction(producedName)
	require.True(t, ok)
	require.Equal(t, uint32(20), produced.Info().DocsTotal)
	release()

	for _, name := range sources {
		_, release, ok = fm.AcquireFraction(name)
		require.False(t, ok)
		release()
	}
	require.NoFileExists(t, task.filename+consts.ReplacementPlan)
	require.NoFileExists(t, task.filename+consts.ReplacementPlanTmp)
	require.Len(t, fm.SealedFractionsSnapshot(), 1)
}

func TestPlannerReleasesReplacementOnError(t *testing.T) {
	fm, _, stopManager := setupPlannerManager(t)
	defer stopManager()

	p := newTestPlanner(t, fm)
	defer p.stop()
	sources := plannerSourceNames(fm)

	task, ok := p.pick()
	require.True(t, ok)

	task.onComplete(nil, errors.New("merge failed"))

	for _, name := range sources {
		_, release, ok := fm.AcquireFraction(name)
		require.True(t, ok)
		release()
	}
	require.NoFileExists(t, task.filename+consts.ReplacementPlan)
	require.NoFileExists(t, task.filename+consts.ReplacementPlanTmp)
	require.Len(t, fm.SealedFractionsSnapshot(), 2)
}

func setupPlannerManager(t *testing.T) (*fracmanager.FracManager, common.SealParams, func()) {
	t.Helper()
	cfg := &fracmanager.Config{
		DataDir:   t.TempDir(),
		FracSize:  uint64(units.MiB),
		TotalSize: uint64(10 * units.MiB),
		CacheSize: uint64(units.MiB),
		Fraction:  frac.Config{SkipSortDocs: true},
	}
	fm, stop, err := fracmanager.New(t.Context(), cfg, nil, testSkipMaskProvider{})
	require.NoError(t, err)

	for batch := range 2 {
		dp := indexer.NewTestDocProvider()
		for i := range 10 {
			doc := []byte(`{"timestamp":0,"message":"msg"}`)
			dp.Append(doc, seq.SimpleID(int64(batch*10+i+1)), "service:100500", "k8s_pod", "_all_:")
		}
		docs, metas := dp.Provide()
		require.NoError(t, fm.Append(t.Context(), docs, metas))
		fm.WaitIdleForTests()
		fm.SealForcedForTests()
	}

	return fm, cfg.SealParams, stop
}

func newTestPlanner(t *testing.T, fm *fracmanager.FracManager) *planner {
	t.Helper()
	return NewPlanner(t.Context(), fm, Config{
		MergeTrigger:     2,
		MergeFanIn:       10,
		MergeFanOutSize:  uint64(10 * units.MiB),
		BucketLowerbound: 0,
		BucketUpperbound: 1000,
		TimeWindow:       time.Hour,
	})
}

func plannerSourceNames(fm *fracmanager.FracManager) []string {
	fractions := fm.SealedFractionsSnapshot()
	result := make([]string, len(fractions))
	for i, f := range fractions {
		result[i] = f.Info().Name()
	}
	return result
}

type testSkipMaskProvider struct{}

func (testSkipMaskProvider) GetIDsIteratorByFrac(_ string, _, _ uint32, reverse bool) (node.Node, bool, func() error, error) {
	return node.NewStatic([]uint32{}, !reverse), false, func() error { return nil }, nil
}

func (testSkipMaskProvider) GetIDsBitmapByFrac(_ string, _, _ uint32) (*roaring.Bitmap, error) {
	return nil, nil
}

func (testSkipMaskProvider) RefreshFrac(_ frac.Fraction) {}
func (testSkipMaskProvider) RemoveFrac(_ string)         {}
