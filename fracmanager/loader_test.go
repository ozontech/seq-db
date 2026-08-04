package fracmanager

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
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
		assert.Equal(t, actives[i].Info().Name(), sealed[i].Info().Name(), "fraction %d should have the same name", i)
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
		expectedSealed[s.Info().Name()] = s
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
	assert.Equal(t, a.BaseFileName, actives[0].BaseFileName, "must be the same name")
	assert.Empty(t, expectedSealed, "we don't expect any more sealed fractions")
	assert.Empty(t, expectedRemote, "we don't expect any more remote fractions")
}

// createEmptyRemoteFile creates an empty .remote marker file on disk.
func createEmptyRemoteFile(t testing.TB, basePath string) {
	t.Helper()

	err := os.WriteFile(basePath+consts.RemoteFractionSuffix, nil, 0o644)
	require.NoError(t, err)
}

// TestDiscover_RemoteInfoExists verifies that a fraction with .remote-info is detected
// as remote with the new split format (no S3 request needed).
// No .frac-cache
func TestDiscover_RemoteInfoExists(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// Create a sealed fraction and offload it — this creates .remote-info on disk
	// and uploads all files to S3.
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10)
	s, err := fp.Seal(a)
	require.NoError(t, err)

	r, err := fp.Offload(t.Context(), s)
	require.NoError(t, err)
	require.NotNil(t, r)
	s.Suicide()

	// Now discover from FS.
	actives, locals, remotes, err := loader.discover(t.Context())
	require.NoError(t, err)

	assert.Empty(t, actives, "no active fractions expected")
	assert.Empty(t, locals, "no local fractions expected")
	require.Len(t, remotes, 1, "one remote fraction expected")

	remote := remotes[0]
	assert.Equal(t, r.Info().Name(), remote.Info().Name(), "remote fraction name should match")
	assert.False(t, remote.IsSingleIndex(), "remote fraction with .remote-info should be non-legacy")
	assert.True(t, util.FileExists(remote.BaseFileName+consts.RemoteFractionInfoSuffix), "file .remote-info must exists")
}

// TestDiscover_EmptyRemote_NewIndex verifies that a fraction with empty .remote
// and no .index in S3 (but split files exist) is detected as non-legacy remote.
// Uses a real offloaded fraction, then replaces .remote-info with empty .remote.
// No .frac-cache
func TestDiscover_EmptyRemote_NewIndex(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// Create a sealed fraction and offload it — this creates real files in S3.
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10)
	s, err := fp.Seal(a)
	require.NoError(t, err)

	r, err := fp.Offload(t.Context(), s)
	require.NoError(t, err)
	require.NotNil(t, r)
	s.Suicide()

	basePath := r.BaseFileName

	// Remove .remote-info and create empty .remote marker instead.
	err = os.Remove(basePath + consts.RemoteFractionInfoSuffix)
	require.NoError(t, err)
	createEmptyRemoteFile(t, basePath)

	// Discover from FS.
	actives, locals, remotes, err := loader.discover(t.Context())
	require.NoError(t, err)

	assert.Empty(t, actives, "no active fractions expected")
	assert.Empty(t, locals, "no local fractions expected")
	require.Len(t, remotes, 1, "one remote fraction expected")

	remote := remotes[0]
	assert.Equal(t, r.Info().Name(), remote.Info().Name(), "remote fraction name should match")
	assert.False(t, remote.IsSingleIndex(), "remote fraction without .index in S3 should be non-legacy")
}

// TestDiscover_EmptyRemote_CacheLegacy verifies that a fraction with empty .remote
// and cached Info with BinaryDataVer < V3 is detected as legacy remote.
func TestDiscover_EmptyRemote_CacheLegacy(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// Create a sealed fraction and offload it.
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10)
	s, err := fp.Seal(a)
	require.NoError(t, err)

	r, err := fp.Offload(t.Context(), s)
	require.NoError(t, err)
	require.NotNil(t, r)
	s.Suicide()

	basePath := r.BaseFileName
	baseName := r.Info().Name()

	// Remove .remote-info and create empty .remote marker instead.
	err = os.Remove(basePath + consts.RemoteFractionInfoSuffix)
	require.NoError(t, err)
	createEmptyRemoteFile(t, basePath)

	// Add cached Info with BinaryDataVer < V3 (simulating legacy)
	// and IndexOnDisk > 0 so NewRemote fast path (info.IndexOnDisk > 0) works.
	cachedInfo := &common.Info{
		Path:          basePath,
		DocsTotal:     r.Info().DocsTotal,
		BinaryDataVer: config.BinaryDataV2, // < V3 — legacy
		IndexOnDisk:   4096,                // > 0 — enables fast path in NewRemote
	}
	loader.infoCache.Add(cachedInfo)
	err = loader.infoCache.SyncWithDisk()
	require.NoError(t, err)

	// Discover from FS.
	actives, locals, remotes, err := loader.discover(t.Context())
	require.NoError(t, err)

	assert.Empty(t, actives, "no active fractions expected")
	assert.Empty(t, locals, "no local fractions expected")
	require.Len(t, remotes, 1, "one remote fraction expected")

	remote := remotes[0]
	assert.Equal(t, baseName, remote.Info().Name(), "remote fraction name should match")
	assert.True(t, remote.IsSingleIndex(), "remote fraction with cached BinaryDataVer<V3 should be legacy")
}

// TestDiscover_EmptyRemote_CacheNew verifies that a fraction with empty .remote
// and cached Info with BinaryDataVer >= V3 is detected as non-legacy remote.
// Since split format is known from cache, no S3 request should be made.
func TestDiscover_EmptyRemote_CacheNew(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// Create a sealed fraction and offload it.
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10)
	s, err := fp.Seal(a)
	require.NoError(t, err)

	// Add cached Info and sync to disk so loadedInfoCache inside discover() picks it up.
	loader.infoCache.Add(s.Info())
	err = loader.infoCache.SyncWithDisk()
	require.NoError(t, err)

	// Offload and remove localy
	r, err := fp.Offload(t.Context(), s)
	require.NoError(t, err)
	require.NotNil(t, r)
	s.Suicide()

	// Remove .remote-info and create empty .remote marker instead.
	basePath := r.BaseFileName
	err = os.Remove(basePath + consts.RemoteFractionInfoSuffix)
	require.NoError(t, err)
	createEmptyRemoteFile(t, basePath)

	// Discover from FS.
	actives, locals, remotes, err := loader.discover(t.Context())
	require.NoError(t, err)

	assert.Empty(t, actives, "no active fractions expected")
	assert.Empty(t, locals, "no local fractions expected")
	require.Len(t, remotes, 1, "one remote fraction expected")

	remote := remotes[0]
	assert.Equal(t, r.Info().Name(), remote.Info().Name(), "remote fraction name should match")
	assert.False(t, remote.IsSingleIndex(), "remote fraction with cached BinaryDataVer>=V3 should be non-legacy")
}

// TestLoadRemote_Legacy verifies loading a legacy remote fraction using cached Info
// with IndexOnDisk > 0 (fast path — no S3 request for info loading).
func TestLoadRemote_Legacy(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	baseName := "seq-db-TESTLEGACYLOAD"
	basePath := filepath.Join(fp.config.DataDir, baseName)

	// Create cached Info with IndexOnDisk > 0 (legacy, fast path).
	cachedInfo := &common.Info{
		Path:          basePath,
		BinaryDataVer: config.BinaryDataV2,
		DocsTotal:     100,
		IndexOnDisk:   4096,
	}
	loadedInfoCache := NewFracInfoCacheFromDisk(loader.infoCache.fullPath)
	loadedInfoCache.Add(cachedInfo)

	remote := loader.loadRemote(t.Context(), basePath, loadedInfoCache)
	require.NotNil(t, remote)
	assert.True(t, remote.IsSingleIndex(), "should be legacy")
}

// TestLoadRemote_NewFormat verifies loading a new-format remote fraction with .remote-info.
func TestLoadRemote_NewFormat(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// Create a sealed fraction and offload it.
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10)
	s, err := fp.Seal(a)
	require.NoError(t, err)

	r, err := fp.Offload(t.Context(), s)
	require.NoError(t, err)
	require.NotNil(t, r)
	s.Suicide()

	loadedInfoCache := NewFracInfoCacheFromDisk(loader.infoCache.fullPath)
	remote := loader.loadRemote(t.Context(), r.BaseFileName, loadedInfoCache)
	require.NotNil(t, remote)
	assert.False(t, remote.IsSingleIndex(), "should be non-legacy")
	assert.Equal(t, r.Info().Name(), remote.Info().Name(), "name should match")
}

// TestLoadRemote_RemoteInfoFallback verifies loading a remote fraction where
// .remote-info is missing but .info exists in S3.
// Uses a real offloaded fraction to ensure valid .info file in S3.
func TestLoadRemote_RemoteInfoFallback(t *testing.T) {
	fp, loader, tearDown := setupLoaderTest(t, nil)
	defer tearDown()

	// Create a sealed fraction and offload it — this uploads valid files to S3.
	a := fp.CreateActive()
	appendDocsToActive(t, a, 10)
	s, err := fp.Seal(a)
	require.NoError(t, err)

	r, err := fp.Offload(t.Context(), s)
	require.NoError(t, err)
	require.NotNil(t, r)
	s.Suicide()

	basePath := r.BaseFileName

	// Remove .remote-info so loadInfo() falls back to S3.
	err = os.Remove(basePath + consts.RemoteFractionInfoSuffix)
	require.NoError(t, err)

	loadedInfoCache := NewFracInfoCacheFromDisk(loader.infoCache.fullPath)
	remote := loader.loadRemote(t.Context(), basePath, loadedInfoCache)
	require.NotNil(t, remote)
	assert.False(t, remote.IsSingleIndex(), "should be non-legacy")
	assert.Equal(t, r.Info().Name(), remote.Info().Name(), "name should match")
}
