package fracmanager

import (
	"fmt"
	"math/rand"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
)

func setupS3Client(t testing.TB) (*s3.Client, func()) {
	s3Backend := s3mem.New()
	s3server := httptest.NewServer(gofakes3.New(s3Backend).Server())

	bucketName := fmt.Sprintf("bucket_%s_%d_%d", t.Name(), time.Now().UnixMilli(), rand.Int())
	err := s3Backend.CreateBucket(bucketName)
	require.NoError(t, err, "create bucket failed")

	s3cli, err := s3.NewClient(s3server.URL, "ACCESS_KEY", "SECRET_KEY", "eu-west-3", bucketName, 3)
	require.NoError(t, err, "s3 client setup failed")

	return s3cli, s3server.Close
}

func setupFractionProvider(t testing.TB, cfg *Config) (*fractionProvider, func()) {
	cfg = setupDataDir(t, cfg)
	rl := storage.NewReadLimiter(1, nil)
	s3cli, stopS3 := setupS3Client(t)
	idx, stopIdx := frac.NewActiveIndexer(1, 1)
	cache := NewCacheMaintainer(uint64(units.MB), uint64(units.MB), nil)
	provider := newFractionProvider(cfg, s3cli, cache, rl, idx, testDocsFilter{})
	return provider, func() {
		stopIdx()
		stopS3()
	}
}

func TestFractionID(t *testing.T) {
	fp := newFractionProvider(nil, nil, nil, nil, nil, nil)
	ulid1 := fp.nextFractionID()
	ulid2 := fp.nextFractionID()
	assert.NotEqual(t, ulid1, ulid2, "ULIDs should be different")
	assert.Equal(t, 26, len(ulid1), "ULID should have length 26")
	assert.Greater(t, ulid2, ulid1)
}
