package fracmanager

import (
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

func setupLifecycle(t testing.TB, cfg *Config) (*lifecycleManager, func()) {
	provider, tearDown := setupFractionProvider(t, cfg)
	dataDir := provider.config.DataDir
	infoCache := NewFracInfoCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))

	registry, err := NewFractionRegistry(provider.CreateActive(), nil, nil)
	assert.NoError(t, err)

	storageState, err := NewStateManager(dataDir, defaultStorageState)
	assert.NoError(t, err)

	lifecycle := newLifecycleManager(infoCache, provider, storageState, registry)

	return lifecycle, tearDown
}

func TestFracInfoCache(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	fillRotateAndCheck := func(names map[string]struct{}) {
		time.Sleep(time.Millisecond * 10)

		appender := lc.registry.appender()
		appendDocsToActive(t, appender.Active, 10+rand.Intn(10))

		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()

		info := appender.Info()
		_, ok := lc.infoCache.Get(info.Name())
		assert.True(t, ok)

		names[info.Name()] = struct{}{}
	}

	first := map[string]struct{}{}
	for range 10 {
		fillRotateAndCheck(first)
	}
	halfSize := lc.registry.statistics().TotalSizeOnDiskLocal()

	second := map[string]struct{}{}
	for range 10 {
		fillRotateAndCheck(second)
	}
	total := lc.registry.statistics().TotalSizeOnDiskLocal()

	wg := sync.WaitGroup{}
	lc.cleanLocal(total-halfSize, &wg)
	wg.Wait()

	for n := range first {
		_, ok := lc.infoCache.Get(n)
		assert.False(t, ok, "expect the first part to be deleted")
	}

	for n := range second {
		_, ok := lc.infoCache.Get(n)
		assert.True(t, ok, "expect the second part to still be present")
	}
}

func TestCapacityExceeded(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	const fracsCount = 10

	fillAndRotate := func() {
		appender := lc.registry.appender()
		appendDocsToActive(t, appender.Active, 10+rand.Intn(10))

		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()
	}

	assert.False(t, lc.flags.IsCapacityExceeded(), "expect data dir is empty")

	// make some fracs
	for range fracsCount {
		fillAndRotate()
	}
	assert.False(t, lc.flags.IsCapacityExceeded(), "there should be no deletions and the flag is false")

	total := lc.registry.statistics().TotalSizeOnDiskLocal()

	wg := sync.WaitGroup{}
	lc.cleanLocal(total, &wg)
	wg.Wait()

	assert.Equal(t, fracsCount, lc.registry.statistics().sealed.count, "as much as was added, so much should be")
	assert.False(t, lc.flags.IsCapacityExceeded(), "there should still be no deletions, and the flag is false")

	lc.cleanLocal(total-1, &wg)
	wg.Wait()

	assert.Equal(t, fracsCount-1, lc.registry.statistics().sealed.count, "expect one less")
	assert.True(t, lc.flags.IsCapacityExceeded(), "the flag must be true now")
}

func TestOldestMetrics(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	const fracsCount = 10
	fillAndRotate := func() {
		appender := lc.registry.appender()
		appendDocsToActive(t, appender.Active, 10+rand.Intn(10))
		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()
	}

	firstFracTime := lc.registry.appender().Info().CreationTime
	for range fracsCount {
		fillAndRotate()
	}

	// Check state after initial rotations
	assert.Equal(t, firstFracTime, lc.registry.oldestTotal(), "should point to the very first fraction when all data is local")
	assert.Equal(t, firstFracTime, lc.registry.oldestLocal(), "should point to the first fraction when nothing is offloaded")

	halfSize := lc.registry.statistics().TotalSizeOnDiskLocal()

	halfwayFracTime := lc.registry.appender().Info().CreationTime
	for range fracsCount {
		fillAndRotate()
	}

	total := lc.registry.statistics().TotalSizeOnDiskLocal()

	wg := sync.WaitGroup{}
	lc.offloadLocal(t.Context(), total-halfSize, 0, &wg)
	wg.Wait()

	// Check state after offloading
	assert.NotEqual(t, firstFracTime, halfwayFracTime, "expect different creation times")
	assert.Equal(t, firstFracTime, lc.registry.oldestTotal(), "should still reference the first fraction after offload")
	assert.Equal(t, halfwayFracTime, lc.registry.oldestLocal(), "should point to the oldest remaining local fraction after offload")
}

func TestPendingDestroy(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	const (
		fracsCount  = 10
		docsPerFrac = 10
	)
	// appending docs to `fracsCount` fractions where the last is active and the rest are sealed
	wg := sync.WaitGroup{}
	for range fracsCount - 1 {
		appendDocsToActive(t, lc.registry.appender().Active, docsPerFrac)
		lc.rotate(0, &wg)
	}
	appendDocsToActive(t, lc.registry.appender().Active, docsPerFrac)

	// wait sealing complete
	wg.Wait()

	// take all fracs to search
	fractions1, release1 := lc.registry.acquireAllFractions()

	// delete all sealing fracs
	lc.cleanLocal(lc.registry.appender().Info().FullSize(), &wg)

	var (
		beforeRelease time.Time
		afterCleanup  time.Time
	)

	cleanup := sync.WaitGroup{}
	cleanup.Add(1)
	go func() {
		// cleanup is pending, so run it in a goroutine
		// waiting for cleanup to finish
		defer cleanup.Done()
		wg.Wait()
		afterCleanup = time.Now()
	}()

	queryAst, err := parser.ParseSeqQL("*", seq.Mapping{})
	require.NoError(t, err, "failed to parse query")
	params := processor.SearchParams{
		AST:   queryAst.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: math.MaxInt32,
	}

	for _, f := range fractions1 {
		qpr, err := f.Search(t.Context(), params)
		assert.NoError(t, err, "failed to search")
		assert.Equal(t, docsPerFrac, len(qpr.IDs))
	}

	beforeRelease = time.Now()
	release1()

	cleanup.Wait()
	assert.Less(t, beforeRelease, afterCleanup, "we expect cleanup to happen after release")

	fractions2, release2 := lc.registry.acquireAllFractions()

	assert.Len(t, fractions2, 1, "only one active fraction should remain")
	singleName := fractions2[0].Info().Name()

	for _, f := range fractions1 {
		if f.Info().Name() == singleName {
			continue
		}
		assert.Panics(t, func() {
			_, _ = f.Search(t.Context(), params)
		}, "searching by destroyed faction is expected to trigger a panic")
	}
	release2()
}
