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
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
)

const fileBasePattern = "seq-db-"

type fractionProvider struct {
	s3cli         *s3.Client
	config        *Config
	cacheProvider *CacheMaintainer
	activeIndexer *frac.ActiveIndexer
	readLimiter   *storage.ReadLimiter
	ulidEntropy   io.Reader
}

func newFractionProvider(
	cfg *Config, s3cli *s3.Client, cp *CacheMaintainer,
	readLimiter *storage.ReadLimiter, indexer *frac.ActiveIndexer,
) *fractionProvider {
	return &fractionProvider{
		s3cli:         s3cli,
		config:        cfg,
		cacheProvider: cp,
		activeIndexer: indexer,
		readLimiter:   readLimiter,
		ulidEntropy:   ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
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
	)
}

func (fp *fractionProvider) NewSealed(name string, cachedInfo *common.Info) *frac.Sealed {
	return frac.NewSealed(
		name,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		cachedInfo,
		&fp.config.Fraction,
	)
}

func (fp *fractionProvider) NewSealedPreloaded(name string, preloadedData *sealed.PreloadedData) *frac.Sealed {
	return frac.NewSealedPreloaded(
		name,
		preloadedData,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		&fp.config.Fraction,
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
	)
}

// This method is not thread safe. Use consciously to avoid race
func (fp *fractionProvider) nextFractionID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), fp.ulidEntropy).String()
}

func (fp *fractionProvider) GenerateActive() *frac.Active {
	filePath := fileBasePattern + fp.nextFractionID()
	baseFilePath := filepath.Join(fp.config.DataDir, filePath)
	return fp.NewActive(baseFilePath)
}

func (fp *fractionProvider) Seal(active *frac.Active) (*frac.Sealed, error) {
	src, err := frac.NewActiveSealingSource(active, fp.config.SealParams)
	if err != nil {
		return nil, nil
	}
	preloaded, err := sealing.Seal(src, fp.config.SealParams)
	if err != nil {
		return nil, nil
	}
	sealed := fp.NewSealedPreloaded(active.BaseFileName, preloaded)

	return sealed, nil
}

func (fp *fractionProvider) Offload(ctx context.Context, sealed *frac.Sealed) (*frac.Remote, error) {
	mustBeOffloaded, err := sealed.Offload(ctx, s3.NewUploader(fp.s3cli))
	if err != nil {
		return nil, err
	}
	if !mustBeOffloaded {
		return nil, nil
	}
	info := sealed.Info()
	return fp.NewRemote(ctx, info.Path, info), nil
}
