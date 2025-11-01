package fracmanager

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

func setupDataDir(t testing.TB, cfg *Config) *Config {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	cfg.Fraction.SkipFsync = true
	return cfg
}

func setupFracManager(t testing.TB, cfg *Config) (*Config, *FracManager, func()) {
	cfg = setupDataDir(t, cfg)
	fm, tearDown, err := New(t.Context(), cfg, nil)
	assert.NoError(t, err)
	return cfg, fm, tearDown
}

func addDummyDoc(t *testing.T, fm *FracManager, dp *frac.DocProvider, seqID seq.ID) {
	doc := []byte("document")
	dp.Append(doc, nil, seqID, seq.Tokens("service:100500", "k8s_pod", "_all_:"))
	docs, metas := dp.Provide()
	err := fm.Append(context.Background(), docs, metas)
	assert.NoError(t, err)
}

func MakeSomeFractions(t *testing.T, fm *FracManager) {
	dp := frac.NewDocProvider()
	addDummyDoc(t, fm, dp, seq.SimpleID(1))
	fm.SealForcedForTests()

	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.SealForcedForTests()

	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(3))
}

func TestCleanUp(t *testing.T) {
	cfg := &Config{
		FracSize:  256 * uint64(units.KiB),
		TotalSize: 1 * uint64(units.MiB),
		Fraction: frac.Config{
			SkipFsync:    true,
			SkipSortDocs: true,
		},
		MaintenanceDelay: 1 * time.Millisecond,
	}

	// first start
	cfg.MinSealFracSize = math.MaxInt64 // to ensure that the frac will not be sealed on shutdown
	cfg, fm, tearDown := setupFracManager(t, cfg)

	MakeSomeFractions(t, fm)

	first := fm.Fractions()[0].(*fractionProxy).impl.(*frac.Sealed)
	first.Suicide()

	second := fm.Fractions()[1].(*fractionProxy).impl.(*frac.Sealed)
	second.Suicide()

	activeName := fm.Fractions()[2].Info().Name()

	tearDown()

	// second start
	cfg.MinSealFracSize = 1 // to ensure that the frac will be sealed on shutdown
	cfg, fm, tearDown = setupFracManager(t, cfg)

	assert.Equal(t, 1, len(fm.Fractions()), "third fraction should be single")
	assert.Equal(t, activeName, fm.Fractions()[0].Info().Name(), "third fraction should be first now")
	_, ok := fm.Fractions()[0].(*fractionProxy).impl.(*frac.Active)
	assert.True(t, ok, "third fraction should be active")

	tearDown()

	// third start
	_, fm, tearDown = setupFracManager(t, cfg)

	assert.Equal(t, 2, len(fm.Fractions()), "third fraction should be rotated")
	_, ok = fm.Fractions()[0].(*fractionProxy).impl.(*frac.Sealed)
	assert.True(t, ok, "third fraction should be sealed")
	assert.Equal(t, activeName, fm.Fractions()[0].Info().Name(), "third fraction should be rotated")
	assert.Equal(t, uint32(0), fm.Fractions()[1].Info().DocsTotal, "active fraction should be empty")
	_, ok = fm.Fractions()[1].(*fractionProxy).impl.(*frac.Active)
	assert.True(t, ok, "new fraction should be active")

	tearDown()
}
