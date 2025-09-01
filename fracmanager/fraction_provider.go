package fracmanager

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/storage/s3"
)

var storeBytesRead = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "seq_db_store",
	Subsystem: "common",
	Name:      "bytes_read",
})

type fractionProvider struct {
	s3cli         *s3.Client
	config        *frac.Config
	cacheProvider *CacheMaintainer
	activeIndexer *frac.ActiveIndexer
	readLimiter   *storage.ReadLimiter
}

func newFractionProvider(
	c *frac.Config, s3cli *s3.Client, cp *CacheMaintainer,
	readerWorkers, indexWorkers int,
) *fractionProvider {
	ai := frac.NewActiveIndexer(indexWorkers, indexWorkers)
	ai.Start() // first start indexWorkers to allow active frac replaying

	return &fractionProvider{
		s3cli:         s3cli,
		config:        c,
		cacheProvider: cp,
		activeIndexer: ai,
		readLimiter:   storage.NewReadLimiter(readerWorkers, storeBytesRead),
	}
}

func (fp *fractionProvider) NewActive(name string) *frac.Active {
	return frac.NewActive(
		name,
		fp.activeIndexer,
		fp.readLimiter,
		fp.cacheProvider.CreateDocBlockCache(),
		fp.cacheProvider.CreateSortDocsCache(),
		fp.config,
	)
}

func (fp *fractionProvider) NewSealed(name string, cachedInfo *frac.Info) *frac.Sealed {
	return frac.NewSealed(
		name,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		cachedInfo,
		fp.config,
	)
}

func (fp *fractionProvider) NewSealedPreloaded(name string, preloadedData *frac.PreloadedData) *frac.Sealed {
	return frac.NewSealedPreloaded(
		name,
		preloadedData,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		fp.config,
	)
}

func (fp *fractionProvider) NewRemote(
	ctx context.Context, name string, cachedInfo *frac.Info,
) *frac.Remote {
	return frac.NewRemote(
		ctx,
		name,
		fp.readLimiter,
		fp.cacheProvider.CreateIndexCache(),
		fp.cacheProvider.CreateDocBlockCache(),
		cachedInfo,
		fp.config,
		fp.s3cli,
	)
}

func (fp *fractionProvider) Stop() {
	fp.activeIndexer.Stop()
}
