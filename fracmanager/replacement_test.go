package fracmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/sealing"
)

func TestReplacementCommit(t *testing.T) {
	for _, sourceCount := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d_sources", sourceCount), func(t *testing.T) {
			fm, tearDown := setupReplacementTest(t)
			defer tearDown()

			sourceNames := make([]string, sourceCount)
			for i := range sourceNames {
				sourceNames[i] = addReplacementSource(t, fm, 10+i)
			}

			producedPath := fm.FractionName()
			r, err := fm.StartReplacement(producedPath, sourceNames)
			require.NoError(t, err)
			require.Len(t, r.Fractions(), sourceCount)

			planData, err := os.ReadFile(producedPath + consts.ReplacementPlan)
			require.NoError(t, err)
			var plan replacementPlan
			require.NoError(t, json.Unmarshal(planData, &plan))
			require.Equal(t, sourceNames, plan.Participants)

			preloaded := buildReplacementOutput(t, fm, producedPath, 7)
			require.NoError(t, r.Commit(preloaded))

			producedName := filepath.Base(producedPath)
			produced, release, ok := fm.AcquireFraction(producedName)
			require.True(t, ok)
			require.Equal(t, uint32(7), produced.Info().DocsTotal)
			release()

			cached, ok := fm.lc.infoCache.Get(producedName)
			require.True(t, ok)
			require.Same(t, preloaded.Info, cached)

			for _, name := range sourceNames {
				_, release, ok = fm.AcquireFraction(name)
				require.False(t, ok)
				release()

				_, ok = fm.lc.infoCache.Get(name)
				require.False(t, ok)
			}

			require.NoFileExists(t, producedPath+consts.ReplacementPlan)
			require.Empty(t, fm.lc.registry.replacing)
			require.Error(t, r.Commit(preloaded))
		})
	}
}

func TestReplacementRelease(t *testing.T) {
	fm, tearDown := setupReplacementTest(t)
	defer tearDown()

	sourceName := addReplacementSource(t, fm, 10)
	producedPath := fm.FractionName()
	r, err := fm.StartReplacement(producedPath, []string{sourceName})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(producedPath+consts.DocsFileSuffix, []byte("partial"), 0o644))
	require.NoError(t, os.WriteFile(producedPath+consts.ReplacementPlanTmp, []byte("partial"), 0o644))
	r.Release()
	r.Release()

	require.NoFileExists(t, producedPath+consts.DocsFileSuffix)
	require.NoFileExists(t, producedPath+consts.ReplacementPlan)
	require.NoFileExists(t, producedPath+consts.ReplacementPlanTmp)
	_, release, ok := fm.AcquireFraction(sourceName)
	require.True(t, ok)
	release()

	next, err := fm.StartReplacement(fm.FractionName(), []string{sourceName})
	require.NoError(t, err)
	next.Release()
}

func TestReplacementCommitValidation(t *testing.T) {
	fm, tearDown := setupReplacementTest(t)
	defer tearDown()

	sourceName := addReplacementSource(t, fm, 10)
	r, err := fm.StartReplacement(fm.FractionName(), []string{sourceName})
	require.NoError(t, err)

	require.Error(t, r.Commit(nil))
	require.Error(t, r.Commit(&sealed.PreloadedData{}))
	require.Error(t, r.Commit(&sealed.PreloadedData{Info: &common.Info{Path: fm.FractionName()}}))

	r.Release()
	require.Error(t, r.Commit(&sealed.PreloadedData{Info: &common.Info{Path: r.producedPath}}))
}

func TestReplacementCommitWaitsForReader(t *testing.T) {
	fm, tearDown := setupReplacementTest(t)
	defer tearDown()

	sourceName := addReplacementSource(t, fm, 10)
	producedPath := fm.FractionName()
	r, err := fm.StartReplacement(producedPath, []string{sourceName})
	require.NoError(t, err)
	preloaded := buildReplacementOutput(t, fm, producedPath, 7)

	_, releaseReader, ok := fm.AcquireFraction(sourceName)
	require.True(t, ok)

	committed := make(chan error, 1)
	go func() {
		committed <- r.Commit(preloaded)
	}()

	producedName := filepath.Base(producedPath)
	visible := assert.Eventually(t, func() bool {
		_, release, ok := fm.AcquireFraction(producedName)
		if ok {
			release()
		}
		return ok
	}, time.Second, time.Millisecond)

	select {
	case err := <-committed:
		require.NoError(t, err)
		require.Fail(t, "commit returned while source reader was active")
	default:
	}

	_, release, ok := fm.AcquireFraction(sourceName)
	require.False(t, ok)
	release()
	require.FileExists(t, r.claimed[0].BaseFileName+consts.DocsFileSuffix)

	releaseReader()
	require.True(t, visible)
	require.NoError(t, <-committed)
	require.NoFileExists(t, r.claimed[0].BaseFileName+consts.DocsFileSuffix)
}

func setupReplacementTest(t *testing.T) (*FracManager, func()) {
	t.Helper()
	lc, tearDown := setupLifecycle(t, nil)
	return &FracManager{lc: lc}, tearDown
}

func addReplacementSource(t *testing.T, fm *FracManager, docCount int) string {
	t.Helper()
	appender := fm.lc.registry.appender()
	appendDocsToActive(t, appender.Active, docCount)

	wg := sync.WaitGroup{}
	fm.lc.rotate(0, &wg)
	wg.Wait()
	return appender.Info().Name()
}

func buildReplacementOutput(t *testing.T, fm *FracManager, producedPath string, docCount int) *sealed.PreloadedData {
	t.Helper()
	active := fm.lc.provider.NewActive(producedPath)
	appendDocsToActive(t, active, docCount)

	src, err := frac.NewActiveSealingSource(active, fm.lc.provider.config.SealParams)
	require.NoError(t, err)
	preloaded, err := sealing.Seal(src, fm.lc.provider.config.SealParams)
	require.NoError(t, err)
	active.Release()
	return preloaded
}
