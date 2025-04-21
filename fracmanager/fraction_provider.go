package fracmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
)

var storeBytesRead = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "seq_db_store",
	Subsystem: "common",
	Name:      "bytes_read",
})

type fractionProvider struct {
	config        *frac.Config
	cacheProvider *CacheMaintainer
	activeIndexer *frac.ActiveIndexer
	readLimiter   *disk.ReadLimiter
}

func newFractionProvider(c *frac.Config, cp *CacheMaintainer, readerWorkers, indexWorkers int) *fractionProvider {
	ai := frac.NewActiveIndexer(indexWorkers, indexWorkers)
	ai.Start() // first start indexWorkers to allow active frac replaying

	return &fractionProvider{
		config:        c,
		cacheProvider: cp,
		activeIndexer: ai,
		readLimiter:   disk.NewReadLimiter(readerWorkers, storeBytesRead),
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

func (fp *fractionProvider) Stop() {
	fp.activeIndexer.Stop()
}

func (fp *fractionProvider) newActiveRef(active *frac.Active) activeRef {
	frac := &proxyFrac{active: active, fp: fp}
	return activeRef{
		frac: frac,
		ref:  &fracRef{instance: frac},
	}
}
