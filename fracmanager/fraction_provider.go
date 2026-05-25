package fracmanager

import (
	"context"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/sealing"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
	"github.com/ozontech/seq-db/util"
)

const fileBasePattern = "seq-db-"

type skipMaskProvider interface {
	GetIDsIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, func() error, error)
	GetIDsBitmapByFrac(fracName string, minLID, maxLID uint32) (*roaring.Bitmap, error)
	RefreshFrac(frac frac.Fraction)
	RemoveFrac(fracName string)
}

// fractionProvider is a factory for creating different types of fractions
// Contains all necessary dependencies for creating and managing fractions
type fractionProvider struct {
	s3cli            *s3.Client           // Client for S3 storage operations
	config           *Config              // Fraction manager configuration
	cacheProvider    *CacheMaintainer     // Cache provider for data access optimization
	activeIndexer    *frac.ActiveIndexer  // Indexer for active fractions
	readLimiter      *storage.ReadLimiter // Read rate limiter
	skipMaskProvider skipMaskProvider

	mu          sync.Mutex
	ulidEntropy io.Reader // Entropy source for ULID generation
}

func newFractionProvider(
	cfg *Config, s3cli *s3.Client, cp *CacheMaintainer,
	readLimiter *storage.ReadLimiter, indexer *frac.ActiveIndexer,
	skipMaskProvider skipMaskProvider,
) *fractionProvider {
	return &fractionProvider{
		s3cli:            s3cli,
		config:           cfg,
		cacheProvider:    cp,
		activeIndexer:    indexer,
		readLimiter:      readLimiter,
		ulidEntropy:      ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
		skipMaskProvider: skipMaskProvider,
	}
}

func (fp *fractionProvider) NewActive(name string) *frac.Active {
	return frac.NewActive(
		name,
		fp.activeIndexer,
		fp.readLimiter,
		fp.cacheProvider.CreateDocBlockCache(),
		fp.cacheProvider.CreateSortDocsCache(),
		&fp.config.Fraction,
		fp.skipMaskProvider,
	)
}

func (fp *fractionProvider) NewSealed(name string, cachedInfo *common.Info, isLegacy bool) *frac.Sealed {
	return frac.NewSealed(
		name,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		cachedInfo, // Preloaded meta information
		&fp.config.Fraction,
		fp.skipMaskProvider,
		isLegacy,
	)
}

func (fp *fractionProvider) NewSealedPreloaded(name string, preloadedData *sealed.PreloadedData) *frac.Sealed {
	return frac.NewSealedPreloaded(
		name,
		preloadedData, // Data already loaded into memory
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		&fp.config.Fraction,
		fp.skipMaskProvider,
	)
}

func (fp *fractionProvider) NewRemote(ctx context.Context, name string, cachedInfo *common.Info, isLegacy bool) *frac.Remote {
	return frac.NewRemote(
		ctx,
		name,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		cachedInfo,
		&fp.config.Fraction,
		fp.s3cli,
		fp.skipMaskProvider,
		isLegacy,
	)
}

// nextFractionID generates a unique identifier for a new fraction
// IMPORTANT: This method is not thread-safe. When used in concurrent environments,
// external synchronization must be provided to avoid ID collisions
func (fp *fractionProvider) nextFractionID() string {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), fp.ulidEntropy).String()
}

// CreateActive creates a new active fraction with auto-generated filename
// Filename pattern: base_pattern + ULID
func (fp *fractionProvider) CreateActive() *frac.Active {
	filePath := fileBasePattern + fp.nextFractionID()
	baseFilePath := filepath.Join(fp.config.DataDir, filePath)
	return fp.NewActive(baseFilePath)
}

// Seal converts an active fraction to a sealed one
// Process includes sorting, indexing, and data optimization for reading
func (fp *fractionProvider) Seal(a *frac.Active) (*frac.Sealed, error) {
	sealsTotal.Inc()
	now := time.Now()

	src, err := frac.NewActiveSealingSource(a, fp.config.SealParams)
	if err != nil {
		return nil, err
	}

	params := fp.config.SealParams
	// NOTE(dkharms): If compaction is enabled we do not want to waste CPU on compression.
	//
	// Sealed fractions will be picked up by compaction workers almost instantly,
	// and that will trigger compression again.
	if fp.config.CompactionEnabled {
		params = common.SealParams{
			DocBlocksZstdLevel: params.DocBlocksZstdLevel,
			LIDBlockSize:       params.LIDBlockSize,
			DocBlockSize:       params.DocBlockSize,
		}
	}

	preloaded, err := sealing.Seal(src, params)
	if err != nil {
		return nil, err
	}

	s := fp.NewSealedPreloaded(a.BaseFileName, preloaded)
	fp.skipMaskProvider.RefreshFrac(s)

	sealingTime := time.Since(now)
	sealsDoneSeconds.Observe(sealingTime.Seconds())

	logger.Info(
		"fraction sealed",
		zap.String("fraction", filepath.Base(s.BaseFileName)),
		zap.Float64("time_spent_s", util.DurationToUnit(sealingTime, "s")),
	)

	return s, nil
}

// Offload uploads fraction to S3 storage and returns a remote fraction
// IMPORTANT: context controls timeouts and operation cancellation
func (fp *fractionProvider) Offload(ctx context.Context, f *frac.Sealed) (*frac.Remote, error) {
	mustBeOffloaded, err := f.Offload(ctx, s3.NewUploader(fp.s3cli))
	if err != nil {
		return nil, err
	}

	if !mustBeOffloaded {
		return nil, nil
	}

	info := f.Info()
	return fp.NewRemote(ctx, info.Path, info, f.IsLegacy), nil
}
