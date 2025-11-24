package fracmanager

import (
	"testing"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
)

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
	fm, stop, err := New(t.Context(), cfg, nil)
	assert.NoError(t, err)
	return cfg, fm, stop
}

func appendDocsToFracManager(t testing.TB, fm *FracManager, docCount int) {
	dp := indexer.NewTestDocProvider()
	for i := 0; i < docCount; i++ {
		doc := []byte("{\"timestamp\": 0, \"message\": \"msg\"}")
		dp.Append(doc, seq.SimpleID(i), "service:100500", "k8s_pod", "_all_:")
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

	f, release := fm.FractionsSnapshot()
	activeName := f[0].Info().Name()
	release()
	stop()

	// second start
	cfg.MinSealFracSize = 1 // to ensure that the frac will be sealed on shutdown
	cfg, fm, stop = setupFracManager(t, cfg)

	f, release = fm.FractionsSnapshot()
	assert.Equal(t, 1, len(f), "should have one fraction")
	assert.Equal(t, activeName, f[0].Info().Name(), "fraction should have the same name")
	_, ok := f[0].(*frac.Active)
	assert.True(t, ok, "fraction should be active")
	release()
	stop()

	// third start
	_, fm, stop = setupFracManager(t, cfg)

	f, release = fm.FractionsSnapshot()
	assert.Equal(t, 2, len(f), "should have 2 fraction: new active and old sealed")
	_, ok = f[0].(*frac.Sealed)
	assert.True(t, ok, "first fraction should be sealed")
	assert.Equal(t, activeName, f[0].Info().Name(), "sealed fraction should have the same name")
	assert.Equal(t, uint32(0), f[1].Info().DocsTotal, "active fraction should be empty")
	_, ok = f[1].(*frac.Active)
	assert.True(t, ok, "new fraction should be active")
	release()
	stop()
}
