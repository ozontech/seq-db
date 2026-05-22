package fracmanager

import (
	"context"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
)

func setupLoaderTest(t testing.TB, cfg *Config) (*fractionProvider, *Loader, func()) {
	fp, tearDown := setupFractionProvider(t, cfg)
	cfg = fp.config
	ic := NewFracInfoCache(filepath.Join(cfg.DataDir, consts.FracCacheFileSuffix))
	loader := NewLoader(cfg, fp, ic)
	return fp, loader, tearDown
}

func appendDocsToActive(t testing.TB, active *frac.Active, docCount int) {
	dp := indexer.NewTestDocProvider()
	for i := 1; i <= docCount; i++ {
		doc := []byte("{\"timestamp\": 0, \"message\": \"msg\"}")
		dp.Append(doc, seq.SimpleID(int64(i)), "service:100500", "k8s_pod", "_all_:")
	}
	docs, metas := dp.Provide()

	wg := sync.WaitGroup{}
	wg.Add(1)
	err := active.Append(docs, metas, &wg)
	assert.NoError(t, err)
	wg.Wait()
}

func TestReplayWithEmptyActive(t *testing.T) {
	const fracCount = 50

	// setup
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	// fill data
	actives := make([]*frac.Active, 0, fracCount)
	for i := 0; i < fracCount; i++ {
		active := fp.CreateActive()
		appendDocsToActive(t, active, 500+rand.Intn(100))
		actives = append(actives, active)
	}
	actives = append(actives, fp.CreateActive()) // last active frac is now empty

	// replay and seal
	active, sealed, err := loader.replayAndSeal(t.Context(), actives)
	assert.NoError(t, err)

	// check
	assert.Equal(t, fracCount, len(sealed), "should replay same number of fractions")
	for i := 0; i < fracCount; i++ {
		assert.Equal(t, actives[i].Info().Name(), sealed[i].Info().Name(), "fraction %d should have same name", i)
		assert.Equal(t, actives[i].Info().DocsTotal, sealed[i].Info().DocsTotal, "fraction %d should have same doc count", i)
	}
	assert.Equal(t, actives[fracCount].Info().Name(), active.Info().Name(), "last replayed fraction should have the same name as last fraction")
	assert.Equal(t, uint32(0), active.Info().DocsTotal, "last fraction should have no documents")
}

func TestReplayWithMultipleEmpty(t *testing.T) {
	const fracCount = 10

	// setup
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	// fill data
	nonEmpty := make([]*common.Info, 0)
	actives := make([]*frac.Active, 0, fracCount)
	for i := 0; i < fracCount; i++ {
		active := fp.CreateActive()
		if i%3 == 0 {
			appendDocsToActive(t, active, 500+rand.Intn(100))
			nonEmpty = append(nonEmpty, active.Info())
		}
		actives = append(actives, active)
	}
	actives = append(actives, fp.CreateActive()) // last active frac is now empty

	// replay and seal
	active, sealed, err := loader.replayAndSeal(t.Context(), actives)
	assert.NoError(t, err)

	// checks
	assert.Equal(t, len(nonEmpty), len(sealed), "non empty frac count doesn't match")
	for i, info := range nonEmpty {
		assert.Equal(t, info.Name(), sealed[i].Info().Name(), "fraction %d should have same name", i)
		assert.Equal(t, info.DocsTotal, sealed[i].Info().DocsTotal, "fraction %d should have same doc count", i)
	}
	assert.Equal(t, uint32(0), active.Info().DocsTotal, "new active fraction should be empty")
}

func TestReplayMultiple(t *testing.T) {
	const fracCount = 50

	// setup
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	// fill data
	actives := make([]*frac.Active, 0, fracCount)
	for i := 0; i < fracCount; i++ {
		active := fp.CreateActive()
		appendDocsToActive(t, active, 500+rand.Intn(100))
		actives = append(actives, active)
	}
	active := fp.CreateActive()
	appendDocsToActive(t, active, 5)
	actives = append(actives, active)

	// replay and seal
	active, sealed, err := loader.replayAndSeal(t.Context(), actives)
	assert.NoError(t, err)

	// checks
	assert.Equal(t, len(actives), len(sealed)+1, "should replay same number of fractions")
	for i := 0; i < fracCount; i++ {
		// assert.Equal(t, actives[i].Info().Name(), sealed[i].Info().Name(), "fraction %d should have the same name", i)
		assert.Equal(t, actives[i].Info().DocsTotal, sealed[i].Info().DocsTotal, "fraction %d should have the same doc count", i)
	}
	assert.Equal(t, actives[fracCount].Info().Name(), active.Info().Name(), "new active fraction should have the same name")
	assert.Equal(t, uint32(5), active.Info().DocsTotal, "new active fraction should not be empty")
}

func TestReplaySingleEmpty(t *testing.T) {
	// setup
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	// fill data: one empty fraction
	actives := []*frac.Active{fp.CreateActive()}

	// replay and seal
	active, sealed, err := loader.replayAndSeal(t.Context(), actives)
	assert.NoError(t, err)

	// checks
	assert.Equal(t, len(sealed), 0, "no sealed")
	assert.Equal(t, actives[0].Info().Name(), active.Info().Name(), "replayed fraction should have the same name")
	assert.Equal(t, uint32(0), active.Info().DocsTotal, "no docs")
}

func TestReplayContextCancel(t *testing.T) {
	const fracCount = 20

	// setup
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	// fill data
	actives := make([]*frac.Active, 0, fracCount)
	for i := 0; i < fracCount; i++ {
		active := fp.CreateActive()
		appendDocsToActive(t, active, 500+rand.Intn(100))
		actives = append(actives, active)
	}
	actives = append(actives, fp.CreateActive())

	// replay and seal
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Microsecond)
	defer cancel()

	_, _, err := loader.replayAndSeal(ctx, actives)

	// checks
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

func TestReplaySingleNonEmpty(t *testing.T) {
	// setup
	fp, loader, tearDown := setupLoaderTest(t, &Config{ReplayWorkers: 10})
	defer tearDown()

	// fill data
	actives := []*frac.Active{fp.CreateActive()}
	appendDocsToActive(t, actives[0], 500+rand.Intn(100))

	// replay and seal
	active, sealed, err := loader.replayAndSeal(t.Context(), actives)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(sealed), "sealed should be empty")
	assert.Equal(t, active.Info().Name(), actives[0].Info().Name(), "should have the same name")
	assert.Equal(t, active.Info().DocsTotal, actives[0].Info().DocsTotal, "should have the same doc count for replayed frac")
}

func TestDiscover(t *testing.T) {
	const fracCount = 16

	// setup
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// make some sealed fracs
	expectedSealed := map[string]*frac.Sealed{}
	for range fracCount {
		a := fp.CreateActive()
		appendDocsToActive(t, a, 10+rand.Intn(10))

		s, err := fp.Seal(a)
		assert.NoError(t, err)
		assert.Len(t, s, 1)
		a.Release()

		expectedSealed[s[0].Info().Name()] = s[0]
	}

	// make half sealed fracs remote
	expectedRemote := map[string]*frac.Remote{}
	for n, s := range expectedSealed {
		if rand.Intn(2) != 0 {
			continue
		}
		r, err := fp.Offload(t.Context(), s)
		assert.NoError(t, err)
		expectedRemote[n] = r
		s.Suicide()
		delete(expectedSealed, n)
	}

	// make half sealed fracs deleted
	for n, s := range expectedSealed {
		if rand.Intn(2) != 0 {
			continue
		}
		s.Suicide()
		delete(expectedSealed, n)
	}

	// make half remote fracs deleted
	for n, r := range expectedRemote {
		if rand.Intn(2) != 0 {
			continue
		}
		r.Suicide()
		delete(expectedRemote, n)
	}

	// make active
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10+rand.Intn(10))

	// discover from FS
	actives, locals, remotes, err := loader.discover(t.Context())
	assert.NoError(t, err)

	// checks
	for _, s := range locals {
		n := s.Info().Name()
		_, ok := expectedSealed[n]
		delete(expectedSealed, n)
		assert.True(t, ok, "not deleted sealed should be discovered")
	}
	for _, s := range remotes {
		n := s.Info().Name()
		_, ok := expectedRemote[n]
		delete(expectedRemote, n)
		assert.True(t, ok, "not deleted remote should be discovered %s", n)
	}

	assert.Equal(t, 1, len(actives), "only one active should be discovered")
	// assert.Equal(t, a.BaseFileName, actives[0].BaseFileName, "must be the same name")
	assert.Empty(t, expectedSealed, "we don't expect any more sealed fractions")
	assert.Empty(t, expectedRemote, "we don't expect any more remote fractions")
}
