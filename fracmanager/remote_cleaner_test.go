package fracmanager

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/storage/s3"
)

func setupRemoteCleaner(t testing.TB, infoCache *fracInfoCache) (*remoteCleaner, func()) {
	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	cleaner := newRemoteCleaner(infoCache, time.Millisecond)
	cleaner.Start(ctx, wg)
	return cleaner, func() {
		cancel()
		wg.Wait()
	}
}

func setupRemoteCleanerTest(t testing.TB) (*remoteCleaner, *fractionProvider) {
	provider, tearDown := setupFractionProvider(t, nil)
	t.Cleanup(tearDown)
	infoCache := NewFracInfoCache(filepath.Join(provider.config.DataDir, consts.FracCacheFileSuffix))
	cleaner := newRemoteCleaner(infoCache, time.Millisecond)
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
	assert.Zero(t, cleaner.pendingCount)
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
	assert.Equal(t, 1, cleaner.pendingCount)

	busy.Dec()
	cleaner.clean()

	_, pending = cleaner.pending.Load(busy.BaseFileName)
	assert.False(t, pending)
	_, cached = cleaner.infoCache.Get(busy.Info().Name())
	assert.False(t, cached)
	assert.Zero(t, cleaner.pendingCount)
}

func TestRemoteCleanerRetry(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	remote := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(remote.Info())

	broken := &refCountedRemote{Remote: frac.NewRemoteZombie(
		t.Context(),
		remote.BaseFileName,
		newBrokenS3Client(t),
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
	assert.Equal(t, 1, cleaner.pendingCount)

	broken.Remote = remote.Remote
	cleaner.clean()

	_, pending = cleaner.pending.Load(broken.BaseFileName)
	assert.False(t, pending)
	_, cached = cleaner.infoCache.Get(remote.Info().Name())
	assert.False(t, cached)
	assert.Zero(t, cleaner.pendingCount)
}

func TestRemoteCleanerQueueLimit(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	remotes := make([]*refCountedRemote, 0, maxPendingRemotes+1)
	for i := range maxPendingRemotes + 1 {
		remote := &refCountedRemote{Remote: frac.NewRemoteZombie(
			t.Context(), filepath.Join(provider.config.DataDir, fmt.Sprint(i)), provider.s3cli, testSkipMaskProvider{},
		)}
		remote.Inc()
		remotes = append(remotes, remote)
		cleaner.Add(remote)
		cleaner.Add(&refCountedRemote{Remote: remote.Remote})
		assert.Equal(t, i+1 >= maxPendingRemotes, cleaner.IsOverflowed())
	}
	require.Equal(t, maxPendingRemotes+1, cleaner.pendingCount)

	for _, tc := range []struct {
		count      int
		overflowed bool
	}{
		{maxPendingRemotes - 1, true},
		{maxPendingRemotes/2 + 1, true},
		{maxPendingRemotes / 2, false},
		{0, false},
	} {
		for _, remote := range remotes[tc.count:] {
			remote.Dec()
		}
		remotes = remotes[:tc.count]
		cleaner.clean()
		assert.Equal(t, tc.count, cleaner.pendingCount)
		assert.Equal(t, tc.overflowed, cleaner.IsOverflowed(), "pending: %d", tc.count)
	}
}

func TestRemoteCleanerTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	client, err := s3.NewClient("http://"+listener.Addr().String(), "ACCESS_KEY", "SECRET_KEY", "eu-west-3", "bucket")
	require.NoError(t, err)
	remote := &refCountedRemote{Remote: frac.NewRemoteZombie(
		t.Context(), filepath.Join(t.TempDir(), "blocked"), client, testSkipMaskProvider{},
	)}
	cleaner := newRemoteCleaner(nil, time.Millisecond)
	cleaner.ctx = t.Context()
	cleaner.Add(remote)

	started := time.Now()
	cleaner.clean()
	elapsed := time.Since(started)

	assert.GreaterOrEqual(t, elapsed, 5*time.Second)
	assert.Less(t, elapsed, 10*time.Second)
	_, pending := cleaner.pending.Load(remote.BaseFileName)
	assert.True(t, pending)
	assert.Equal(t, 1, cleaner.pendingCount)
}

func TestRemoteCleanerStopsWithReaders(t *testing.T) {
	cleaner, provider := setupRemoteCleanerTest(t)
	remote := makeOffloadedFraction(t, provider)
	cleaner.infoCache.Add(remote.Info())

	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	cleaner.Start(ctx, wg)
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
