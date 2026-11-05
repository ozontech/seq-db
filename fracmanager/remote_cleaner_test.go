package fracmanager

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
)

func setupRemoteCleaner(t testing.TB, infoCache *fracInfoCache) (*remoteCleaner, func()) {
	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	cleaner := newRemoteCleaner(infoCache)
	cleaner.Start(ctx, time.Millisecond, wg)
	return cleaner, func() {
		cancel()
		wg.Wait()
	}
}

func setupRemoteCleanerTest(t testing.TB) (*remoteCleaner, *fractionProvider) {
	provider, tearDown := setupFractionProvider(t, nil)
	t.Cleanup(tearDown)
	infoCache := NewFracInfoCache(filepath.Join(provider.config.DataDir, consts.FracCacheFileSuffix))
	cleaner := newRemoteCleaner(infoCache)
	cleaner.ctx = t.Context()
	return cleaner, provider
}

func makeOffloadedFraction(t testing.TB, provider *fractionProvider) *refCountedRemote {
	a := provider.CreateActive()
	appendDocsToActive(t, a, 10)
	sealed, err := provider.Seal(a)
	require.NoError(t, err)
	remote, err := provider.Offload(t.Context(), sealed)
	require.NoError(t, err)
	return &refCountedRemote{Remote: remote}
}

func TestRemoteCleaner(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	remote := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(remote.Info())

	cleaner.Add(remote)
	cleaner.clean()

	_, pending := cleaner.pending.Load(remote.BaseFileName)
	assert.False(t, pending)
	_, cached := cleaner.infoCache.Get(remote.Info().Name())
	assert.False(t, cached)
}

func TestRemoteCleanerWithReaders(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	busy := makeOffloadedFraction(t, provider)
	other := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(busy.Info())
	cleaner.infoCache.Add(other.Info())

	busy.Inc()
	cleaner.Add(busy)
	cleaner.Add(other)
	cleaner.clean()

	_, pending := cleaner.pending.Load(busy.BaseFileName)
	assert.True(t, pending)
	_, cached := cleaner.infoCache.Get(busy.Info().Name())
	assert.True(t, cached)
	_, pending = cleaner.pending.Load(other.BaseFileName)
	assert.False(t, pending)

	busy.Dec()
	cleaner.clean()

	_, pending = cleaner.pending.Load(busy.BaseFileName)
	assert.False(t, pending)
	_, cached = cleaner.infoCache.Get(busy.Info().Name())
	assert.False(t, cached)
}

func TestRemoteCleanerRetry(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	remote := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(remote.Info())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	broken := &refCountedRemote{Remote: frac.NewRemoteZombie(
		ctx,
		remote.BaseFileName,
		provider.s3cli,
		testSkipMaskProvider{},
	)}

	other := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(other.Info())
	cleaner.Add(broken)
	cleaner.Add(other)
	cleaner.clean()

	_, pending := cleaner.pending.Load(broken.BaseFileName)
	assert.True(t, pending)
	_, cached := cleaner.infoCache.Get(remote.Info().Name())
	assert.True(t, cached)
	_, pending = cleaner.pending.Load(other.BaseFileName)
	assert.False(t, pending)

	broken.Remote = remote.Remote
	cleaner.clean()

	_, pending = cleaner.pending.Load(broken.BaseFileName)
	assert.False(t, pending)
	_, cached = cleaner.infoCache.Get(remote.Info().Name())
	assert.False(t, cached)
}

func TestRemoteCleanerStopsWithReaders(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	remote := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(remote.Info())

	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	cleaner.Start(ctx, time.Millisecond, wg)
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	remote.Inc()
	defer remote.Dec()
	cleaner.Add(remote)
	require.Never(t, func() bool {
		_, found := cleaner.infoCache.Get(remote.Info().Name())
		return !found
	}, 20*time.Millisecond, time.Millisecond)
	stopped := make(chan struct{})
	go func() {
		cancel()
		wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("cleaner is blocked by readers")
	}
}
