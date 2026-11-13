package fracmanager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

type testSkipMaskProvider struct{}

func (testSkipMaskProvider) GetIDsIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, func() error, error) {
	return node.NewStatic([]uint32{}, !reverse), false, func() error { return nil }, nil
}
func (testSkipMaskProvider) GetIDsBitmapByFrac(fracName string, minLID, maxLID uint32) (*roaring.Bitmap, error) {
	return nil, nil
}
func (testSkipMaskProvider) RefreshFrac(_ frac.Fraction) {}
func (testSkipMaskProvider) RemoveFrac(_ string)         {}

func setupDataDir(t testing.TB, cfg *Config) *Config {
	if cfg == nil {
		cfg = &Config{
			Fraction: frac.Config{SkipSortDocs: true},
		}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	return cfg
}

func setupFracManager(t testing.TB, cfg *Config) (*Config, *FracManager, func()) {
	cfg = setupDataDir(t, cfg)
	fm, stop, err := New(t.Context(), cfg, nil, testSkipMaskProvider{})
	assert.NoError(t, err)
	return cfg, fm, stop
}

func appendDocsToFracManager(t testing.TB, fm *FracManager, docCount int) {
	dp := indexer.NewTestDocProvider()
	for i := 0; i < docCount; i++ {
		doc := []byte("{\"timestamp\": 0, \"message\": \"msg\"}")
		dp.Append(doc, seq.SimpleID(int64(i)), "service:100500", "k8s_pod", "_all_:")
	}
	docs, metas := dp.Provide()
	err := fm.Append(t.Context(), docs, metas)
	assert.NoError(t, err)
}

func TestSealingOnShutdown(t *testing.T) {
	cfg := &Config{
		FracSize:  1 * uint64(units.MiB), // to ensure that the frac will not be sealed on maintenance
		TotalSize: 1 * uint64(units.MiB),
		Fraction:  frac.Config{SkipSortDocs: true},
	}

	// first start
	cfg.MinSealFracSize = 0 // to ensure that the frac will not be sealed on shutdown
	cfg, fm, stop := setupFracManager(t, cfg)
	appendDocsToFracManager(t, fm, 10)

	activeName := fm.lc.registry.snapshot.fractions[0].Info().Name()

	stop()

	// second start
	cfg.MinSealFracSize = 1 // to ensure that the frac will be sealed on shutdown
	cfg, fm, stop = setupFracManager(t, cfg)

	allFractions := fm.lc.registry.snapshot.fractions
	assert.Equal(t, 1, len(allFractions), "should have one fraction")
	assert.Equal(t, activeName, allFractions[0].Info().Name(), "fraction should have the same name")
	_, ok := allFractions[0].(*syncAppender)
	assert.True(t, ok, "fraction should be active")
	stop()

	// third start
	_, fm, stop = setupFracManager(t, cfg)

	allFractions = fm.lc.registry.snapshot.fractions
	assert.Equal(t, 2, len(allFractions), "should have 2 fraction: new active and old sealed")
	_, ok = allFractions[0].(*refCountedSealed)
	assert.True(t, ok, "first fraction should be sealed")
	assert.Equal(t, activeName, allFractions[0].Info().Name(), "sealed fraction should have the same name")
	assert.Equal(t, uint32(0), allFractions[1].Info().DocsTotal, "active fraction should be empty")
	_, ok = allFractions[1].(*syncAppender)
	assert.True(t, ok, "new fraction should be active")
	stop()
}

func TestRemoteCleanupOnStartup(t *testing.T) {
	provider, tearDown := setupFractionProvider(t, &Config{
		TotalSize:              uint64(units.MiB),
		OffloadingEnabled:      true,
		RemoteRemoveRetryDelay: time.Millisecond,
		Fraction:               frac.Config{SkipSortDocs: true},
	})
	defer tearDown()

	for _, markerSuffix := range []string{
		consts.RemoteFractionTmpSuffix,
		consts.RemoteFractionDeletingSuffix,
	} {
		t.Run(markerSuffix, func(t *testing.T) {
			remote := makeOffloadedFraction(t, provider)
			basePath := remote.BaseFileName
			for _, suffix := range []string{
				consts.DocsFileSuffix, consts.SdocsFileSuffix, consts.IndexFileSuffix,
				consts.InfoFileSuffix, consts.TokenFileSuffix, consts.OffsetsFileSuffix,
				consts.IDFileSuffix, consts.LIDFileSuffix,
			} {
				if err := os.Remove(basePath + suffix); !os.IsNotExist(err) {
					require.NoError(t, err)
				}
			}
			require.NoError(t, os.Rename(basePath+consts.RemoteFractionSuffix, basePath+markerSuffix))

			fm, stop, err := New(t.Context(), provider.config, provider.s3cli, testSkipMaskProvider{})
			require.NoError(t, err)
			defer stop()

			require.Zero(t, fm.lc.registry.statistics().remotes.count)
			require.Eventually(t, func() bool {
				_, err := os.Stat(basePath + markerSuffix)
				return os.IsNotExist(err)
			}, 5*time.Second, time.Millisecond)
			exists, err := provider.s3cli.Exists(t.Context(), filepath.Base(basePath)+consts.DocsFileSuffix)
			require.NoError(t, err)
			assert.False(t, exists)
		})
	}
}
