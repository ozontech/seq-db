package fracmanager

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
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

	var total uint64

	fillRotateAndCheck := func(names map[string]struct{}) {
		active := lc.registry.Active()
		appendDocsToActive(t, active.instance, 10+rand.Intn(10))

		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()

		info := active.proxy.Info()
		_, ok := lc.infoCache.Get(info.Name())
		assert.True(t, ok)

		total += info.FullSize()
		names[info.Name()] = struct{}{}
	}

	first := map[string]struct{}{}
	for range 10 {
		fillRotateAndCheck(first)
	}
	halfSize := total

	second := map[string]struct{}{}
	for range 10 {
		fillRotateAndCheck(second)
	}

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
	var total uint64

	fillAndRotate := func() {
		active := lc.registry.Active()
		appendDocsToActive(t, active.instance, 10+rand.Intn(10))

		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()

		info := active.proxy.Info()
		total += info.FullSize()
	}

	assert.False(t, lc.flags.IsCapacityExceeded(), "expect data dir is empty")

	// make some fracs
	for range fracsCount {
		fillAndRotate()
	}
	assert.False(t, lc.flags.IsCapacityExceeded(), "there should be no deletions and the flag is false")

	wg := sync.WaitGroup{}
	lc.cleanLocal(total, &wg)
	wg.Wait()

	assert.Equal(t, fracsCount, lc.registry.Stats().sealed.count, "as much as was added, so much should be")
	assert.False(t, lc.flags.IsCapacityExceeded(), "there should still be no deletions, and the flag is false")

	lc.cleanLocal(total-1, &wg)
	wg.Wait()

	assert.Equal(t, fracsCount-1, lc.registry.Stats().sealed.count, "expect one less")
	assert.True(t, lc.flags.IsCapacityExceeded(), "the flag must be true now")
}

func TestOldestMetrics(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	const fracsCount = 10
	var total uint64

	fillAndRotate := func() {
		active := lc.registry.Active()
		appendDocsToActive(t, active.instance, 10+rand.Intn(10))
		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()

		info := active.proxy.Info()
		total += info.FullSize()
	}

	firstFracTime := lc.registry.Active().proxy.Info().CreationTime
	for range fracsCount {
		fillAndRotate()
	}

	// Check state after initial rotations
	assert.Equal(t, firstFracTime, lc.registry.OldestTotal(), "should point to the very first fraction when all data is local")
	assert.Equal(t, firstFracTime, lc.registry.OldestLocal(), "should point to the first fraction when nothing is offloaded")

	halfSize := total
	halfwayFracTime := lc.registry.Active().proxy.Info().CreationTime
	for range fracsCount {
		fillAndRotate()
	}

	wg := sync.WaitGroup{}
	lc.offloadLocal(t.Context(), total-halfSize, &wg)
	wg.Wait()

	// Check state after offloading
	assert.NotEqual(t, firstFracTime, halfwayFracTime, "expect different creation times")
	assert.Equal(t, firstFracTime, lc.registry.OldestTotal(), "should still reference the first fraction after offload")
	assert.Equal(t, halfwayFracTime, lc.registry.OldestLocal(), "should point to the oldest remaining local fraction after offload")
}
