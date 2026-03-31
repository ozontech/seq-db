package fracmanager

import (
	"context"
	"io"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
)

const fileBasePattern = "seq-db-"

type skipMaskProvider interface {
	GetIDsIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, error)
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
	ulidEntropy      io.Reader            // Entropy source for ULID generation
	skipMaskProvider skipMaskProvider
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

func (fp *fractionProvider) NewRemote(ctx context.Context, name string, cachedInfo *common.Info) *frac.Remote {
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
	)
}

// nextFractionID generates a unique identifier for a new fraction
// IMPORTANT: This method is not thread-safe. When used in concurrent environments,
// external synchronization must be provided to avoid ID collisions
func (fp *fractionProvider) nextFractionID() string {
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
func (fp *fractionProvider) Seal(active *frac.Active) (*frac.Sealed, error) {
	src, err := frac.NewActiveSealingSource(active, fp.config.SealParams)
	if err != nil {
		return nil, err
	}
	preloaded, err := sealing.Seal(src, fp.config.SealParams)
	if err != nil {
		return nil, err
	}

	sealedFrac := fp.NewSealedPreloaded(active.BaseFileName, preloaded)
	fp.skipMaskProvider.RefreshFrac(sealedFrac)
	return sealedFrac, nil
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
	return fp.NewRemote(ctx, info.Path, info), nil
}
