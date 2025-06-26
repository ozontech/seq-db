package fracmanager

import (
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var storeBytesRead = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "seq_db_store",
	Subsystem: "common",
	Name:      "bytes_read",
})

type fractionProvider struct {
	config        *frac.Config
	cacheProvider *CacheMaintainer
	indexWorkers  *frac.IndexWorkers
	readLimiter   *disk.ReadLimiter
}

func newFractionProvider(c *frac.Config, cp *CacheMaintainer, readerWorkers, indexWorkers int) *fractionProvider {
	iw := frac.NewIndexWorkers(indexWorkers, indexWorkers)
	iw.Start() // first start indexWorkers to allow active frac replaying

	return &fractionProvider{
		config:        c,
		cacheProvider: cp,
		indexWorkers:  iw,
		readLimiter:   disk.NewReadLimiter(readerWorkers, storeBytesRead),
	}
}

func (fp *fractionProvider) NewActive(name string) *frac.Active {
	return frac.NewActive(
		name,
		fp.indexWorkers,
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
	fp.indexWorkers.Stop()
}

func (fp *fractionProvider) newActiveRef(active *frac.Active) activeRef {
	return activeRef{
		frac: active,
		ref:  &fracRef{instance: active},
	}
}
