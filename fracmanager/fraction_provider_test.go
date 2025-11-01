package fracmanager

import (
	"testing"

	"github.com/alecthomas/units"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/storage"
	"github.com/stretchr/testify/assert"
)

func setupFractionProvider(t testing.TB, cfg *Config) (*fractionProvider, func()) {
	cfg = setupDataDir(t, cfg)
	rl := storage.NewReadLimiter(1, nil)
	idx, stopIdx := frac.NewActiveIndexer(1, 1)
	cache := NewCacheMaintainer(uint64(units.MB), uint64(units.MB), nil)
	provider := newFractionProvider(cfg, nil, cache, rl, idx)
	return provider, stopIdx
}

func TestFractionID(t *testing.T) {
	fp := newFractionProvider(nil, nil, nil, nil, nil)
	ulid1 := fp.nextFractionID()
	ulid2 := fp.nextFractionID()
	assert.NotEqual(t, ulid1, ulid2, "ULIDs should be different")
	assert.Equal(t, 26, len(ulid1), "ULID should have length 26")
	assert.Greater(t, ulid2, ulid1)
}
