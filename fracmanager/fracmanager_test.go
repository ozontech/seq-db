package fracmanager

import (
	"testing"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

type testFilterManager struct{}

func (testFilterManager) GetHideFlagIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, error) {
	return node.NewStatic([]uint32{}, reverse), false, nil
}
func (testFilterManager) RefreshFrac(_ frac.Fraction) {}
func (testFilterManager) RemoveFrac(_ string)         {}

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
	fm, stop, err := New(t.Context(), cfg, nil, testFilterManager{})
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
	activeName := fm.Fractions()[0].Info().Name()
	stop()

	// second start
	cfg.MinSealFracSize = 1 // to ensure that the frac will be sealed on shutdown
	cfg, fm, stop = setupFracManager(t, cfg)

	assert.Equal(t, 1, len(fm.Fractions()), "should have one fraction")
	assert.Equal(t, activeName, fm.Fractions()[0].Info().Name(), "fraction should have the same name")
	_, ok := fm.Fractions()[0].(*fractionProxy).impl.(*frac.Active)
	assert.True(t, ok, "fraction should be active")

	stop()

	// third start
	_, fm, stop = setupFracManager(t, cfg)

	assert.Equal(t, 2, len(fm.Fractions()), "should have 2 fraction: new active and old sealed")
	_, ok = fm.Fractions()[0].(*fractionProxy).impl.(*frac.Sealed)
	assert.True(t, ok, "first fraction should be sealed")
	assert.Equal(t, activeName, fm.Fractions()[0].Info().Name(), "sealed fraction should have the same name")
	assert.Equal(t, uint32(0), fm.Fractions()[1].Info().DocsTotal, "active fraction should be empty")
	_, ok = fm.Fractions()[1].(*fractionProxy).impl.(*frac.Active)
	assert.True(t, ok, "new fraction should be active")

	stop()
}
