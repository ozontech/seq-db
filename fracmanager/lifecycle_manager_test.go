package fracmanager

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
)

func setupLifecycle(t testing.TB, cfg *Config) (*lifecycleManager, func()) {
	provider, tearDown := setupFractionProvider(t, cfg)
	dataDir := provider.config.DataDir
	infoCache := NewFracInfoCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))

	registry, err := NewFractionRegistry(provider.CreateActive(), nil, nil, nil)
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

type remoteSpec struct {
	minutesAgo int // creation time and last-doc time, minutes back from now
	size       uint64
}

// addRemotes creates remote fraction fixtures in the registry. Eviction
// uses Info only (creation time, last-doc time, FullSize), the fractions
// themselves are never searched or loaded. size is the resulting
// FullSize; a zero size gives a 1-byte fraction (the minimal nonzero
// IndexOnDisk skips S3 info loading in NewRemote).
func addRemotes(lc *lifecycleManager, specs ...remoteSpec) {
	lc.registry.mu.Lock()
	defer lc.registry.mu.Unlock()

	now := time.Now()
	for i, s := range specs {
		name := fmt.Sprintf("frac_remote_%d_%d", now.UnixNano(), i)
		info := common.NewInfo(name, max(s.size, 1)-1, 0)
		info.IndexOnDisk = 1

		ts := now.Add(-time.Duration(s.minutesAgo) * time.Minute)
		info.CreationTime = uint64(ts.UnixMilli())
		// remotes are partitioned by last-doc time (aligned up to a
		// minute) - the eviction order comes from it
		info.To = seq.MillisToMID(uint64(ts.UnixMilli()))

		remote := frac.NewRemote(
			context.Background(),
			name,
			nil,
			frac.NewIndexCache(),
			nil,
			info,
			&frac.Config{},
			nil,
			testSkipMaskProvider{},
		)
		lc.registry.remotes.Add(name, &refCountedRemote{Remote: remote})
		lc.registry.stats.remotes.Add(info)
	}
	lc.registry.rebuildSnapshot()
}

// remoteTotal is the size the evictRemote size accounting operates on.
func remoteTotal(stats *registryStats) uint64 {
	return stats.TotalSizeOnDiskRemote() + stats.TotalSizeOnDiskOffloading()
}

func TestEvictRemote(t *testing.T) {
	const fracSize = 100

	tests := []struct {
		name string

		// fixtures: age (minutes back from now) and FullSize of each remote fraction;
		// zero size gives a 1-byte fraction
		specs []remoteSpec

		retention time.Duration
		sizeLimit uint64

		// extra offloading-queue size not backed by any remote fraction;
		// it counts toward the limit but can't be released
		offloadingSize uint64

		wantEvicted int // evicted fraction count
		wantRemotes int // remaining remote fraction count
		// the size accounting the eviction operates on, after the eviction
		checkTotal func(t *testing.T, lc *lifecycleManager)
		// set when an eviction happened: oldestTotal must shift to a younger fraction
		wantOldestShift bool
	}{
		{
			name: "only the fraction older than retention is evicted",
			specs: []remoteSpec{
				{minutesAgo: 60},
				{minutesAgo: 10},
				{minutesAgo: 5},
			},
			retention:       30 * time.Minute,
			wantEvicted:     1,
			wantRemotes:     2,
			wantOldestShift: true,
		},
		{
			name: "the oldest fraction is evicted to fit the size limit",
			specs: []remoteSpec{
				{minutesAgo: 30, size: fracSize},
				{minutesAgo: 20, size: fracSize},
				{minutesAgo: 10, size: fracSize},
			},
			sizeLimit:   fracSize * 2,
			wantEvicted: 1,
			wantRemotes: 2,
			checkTotal: func(t *testing.T, lc *lifecycleManager) {
				assert.Equal(t, uint64(fracSize*2), remoteTotal(&lc.registry.stats))
			},
			wantOldestShift: true,
		},
		{
			name: "the whole oldest partition goes even if the limit fits less",
			specs: []remoteSpec{
				{minutesAgo: 30, size: 1000},
				{minutesAgo: 10, size: 100},
			},
			sizeLimit:   500,
			wantEvicted: 1,
			wantRemotes: 1,
			checkTotal: func(t *testing.T, lc *lifecycleManager) {
				assert.LessOrEqual(t, remoteTotal(&lc.registry.stats), uint64(500))
			},
		},
		{
			name: "no limits configured, nothing is ever evicted",
			specs: []remoteSpec{
				{minutesAgo: 600, size: fracSize},
				{minutesAgo: 5, size: fracSize},
			},
			wantRemotes: 2,
		},
		{
			name: "no size limit configured, only retention",
			specs: []remoteSpec{
				{minutesAgo: 600, size: fracSize},
				{minutesAgo: 5, size: fracSize},
			},
			retention:       30 * time.Minute,
			wantEvicted:     1,
			wantRemotes:     1,
			wantOldestShift: true,
		},
		{
			name: "retention set, all fractions fresh, nothing is evicted",
			specs: []remoteSpec{
				{minutesAgo: 5, size: fracSize},
				{minutesAgo: 3, size: fracSize},
			},
			retention:   time.Hour,
			wantRemotes: 2,
		},
		{
			name: "limit below any achievable total, everything is evicted",
			specs: []remoteSpec{
				{minutesAgo: 30, size: fracSize},
				{minutesAgo: 20, size: fracSize * 2},
			},
			sizeLimit:   1,
			wantEvicted: 2,
			wantRemotes: 0,
		},
		{
			name: "offloading queue size pushes the total over the limit",
			specs: []remoteSpec{
				{minutesAgo: 30, size: fracSize},
				{minutesAgo: 20, size: fracSize},
			},
			offloadingSize: fracSize,
			sizeLimit:      fracSize * 2,
			wantEvicted:    1,
			wantRemotes:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, tearDown := setupLifecycle(t, nil)
			defer tearDown()

			addRemotes(lc, tt.specs...)

			if tt.offloadingSize > 0 {
				offloading := common.NewInfo("frac_offloading", tt.offloadingSize-1, 0)
				offloading.IndexOnDisk = 1 // FullSize == offloadingSize

				lc.registry.mu.Lock()
				lc.registry.stats.offloading.Add(offloading)
				lc.registry.mu.Unlock()
			}

			evicted := lc.registry.evictRemote(tt.retention, tt.sizeLimit)

			require.Len(t, evicted, tt.wantEvicted)
			assert.Equal(t, tt.wantRemotes, lc.registry.statistics().remotes.count)

			if tt.checkTotal != nil {
				tt.checkTotal(t, lc)
			}

			if tt.wantOldestShift {
				require.NotEmpty(t, evicted)
				assert.Greater(t, lc.registry.oldestTotal(), evicted[0].Info().CreationTime,
					"oldestTotal shifted to a younger fraction")
			}
		})
	}
}

func TestCleanRemoteBySizeIntegration(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	const fracsCount = 6

	fillAndRotate := func() {
		appender := lc.registry.appender()
		appendDocsToActive(t, appender.Active, 10)
		wg := sync.WaitGroup{}
		lc.rotate(0, &wg)
		wg.Wait()
	}

	for range fracsCount {
		fillAndRotate()
	}

	// offload half of the local fractions to S3
	localTotal := lc.registry.statistics().TotalSizeOnDiskLocal()
	wg := sync.WaitGroup{}
	lc.offloadLocal(t.Context(), localTotal/2, 0, &wg)
	wg.Wait()

	require.Equal(t, fracsCount/2, lc.registry.statistics().remotes.count,
		"half of the fractions is offloaded")

	// the limit is one byte below the current remote size: the oldest
	// partition must be deleted and the capacity flag set
	wg = sync.WaitGroup{}
	lc.cleanRemote(
		context.Background(),
		0,
		lc.registry.statistics().TotalSizeOnDiskRemote()-1,
		0,
		&wg,
	)
	wg.Wait()

	stats := lc.registry.statistics()
	assert.Less(t, stats.remotes.count, fracsCount/2, "at least one remote fraction is evicted")
	assert.True(t, lc.flags.IsCapacityExceeded(), "capacity exceeded flag is set after remote deletion")
}

// makeOffloadedFraction creates a sealed fraction, offloads it to S3 and
// returns it as a remote reference with a .remote marker on disk.
func makeOffloadedFraction(t testing.TB, lc *lifecycleManager) *refCountedRemote {
	a := lc.provider.CreateActive()
	appendDocsToActive(t, a, 10)

	wg := sync.WaitGroup{}
	lc.rotate(0, &wg)
	wg.Wait()

	sealed, err := lc.provider.Seal(a)
	require.NoError(t, err, "seal failed")

	remote, err := lc.provider.Offload(t.Context(), sealed)
	require.NoError(t, err, "offload failed")
	require.NotNil(t, remote)

	return &refCountedRemote{Remote: remote}
}

// newBrokenS3Client returns an S3 client pointing to a closed address:
// every request fails, simulating remote storage unavailability.
func newBrokenS3Client(t testing.TB) *s3.Client {
	s3cli, err := s3.NewClient(
		"http://127.0.0.1:1", // nothing listens here
		"ACCESS_KEY",
		"SECRET_KEY",
		"eu-west-3",
		"bucket",
	)
	require.NoError(t, err, "broken s3 client setup failed")
	return s3cli
}

func TestRemoveRemoteAsync(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	ref := makeOffloadedFraction(t, lc)
	lc.infoCache.Add(ref.Info())

	wg := sync.WaitGroup{}
	lc.removeRemoteAsync(t.Context(), ref, &wg, time.Second)
	wg.Wait()

	// Remote files are deleted from S3.
	for _, suffix := range []string{
		consts.DocsFileSuffix, consts.SdocsFileSuffix, consts.IndexFileSuffix,
		consts.InfoFileSuffix, consts.TokenFileSuffix, consts.OffsetsFileSuffix,
		consts.IDFileSuffix, consts.LIDFileSuffix,
	} {
		exists, err := lc.provider.s3cli.Exists(t.Context(), filepath.Base(ref.BaseFileName)+suffix)
		require.NoError(t, err)
		assert.False(t, exists, "remote file %s must be deleted", suffix)
	}

	// The .remote marker is removed and the fraction is dropped from the info cache.
	_, err := os.Stat(ref.BaseFileName + consts.RemoteFractionSuffix)
	assert.True(t, os.IsNotExist(err), ".remote marker must be removed")
	_, found := lc.infoCache.Get(ref.Info().Name())
	assert.False(t, found, "fraction must be dropped from the info cache")
}

func TestRemoveRemoteAsyncCanceled(t *testing.T) {
	lc, tearDown := setupLifecycle(t, nil)
	defer tearDown()

	ref := makeOffloadedFraction(t, lc)

	// Replace the fraction with one whose S3 client always fails, so
	// the first attempt fails and the loop exits via ctx instead.
	broken := &refCountedRemote{Remote: frac.NewRemote(
		t.Context(),
		ref.BaseFileName,
		storage.NewReadLimiter(1, nil),
		frac.NewIndexCache(),
		cache.NewConcurrentCache[[]byte](nil, nil),
		ref.Info(),
		&lc.provider.config.Fraction,
		newBrokenS3Client(t),
		testSkipMaskProvider{},
	)}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	wg := sync.WaitGroup{}
	lc.removeRemoteAsync(ctx, broken, &wg, time.Second)
	wg.Wait()

	// The failed attempt committed the deletion intent (marker rename),
	// remote files must survive for the next retry: after restart the
	// fraction is discovered via the .remote.del marker.
	_, err := os.Stat(ref.BaseFileName + consts.RemoteFractionDeletingSuffix)
	assert.NoError(t, err, ".remote.del marker must survive canceled deletion")

	exists, err := lc.provider.s3cli.Exists(t.Context(), filepath.Base(ref.BaseFileName)+consts.DocsFileSuffix)
	require.NoError(t, err)
	assert.True(t, exists, "remote files must survive canceled deletion")
}
