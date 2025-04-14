package fracmanager

import (
	"os"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
)

type builder struct {
	config        frac.Config
	cacheProvider *CacheMaintainer
	readLimiter   *disk.ReadLimiter
	indexWorkers  *frac.IndexWorkers
}

func newBuilder(c frac.Config, cp *CacheMaintainer, rl *disk.ReadLimiter, iw *frac.IndexWorkers) *builder {
	iw.Start() // first start indexWorkers to allow active frac replaying
	return &builder{
		config:        c,
		cacheProvider: cp,
		readLimiter:   rl,
		indexWorkers:  iw,
	}
}

func (b *builder) NewActive(name string) *frac.Active {
	return frac.NewActive(
		name,
		b.indexWorkers,
		b.readLimiter,
		b.cacheProvider.CreateDocBlockCache(),
		b.config,
	)
}

func (b *builder) NewDocBlocksReader(file *os.File) disk.DocBlocksReader {
	return disk.NewDocBlocksReader(b.readLimiter, file)
}

func (b *builder) NewSealed(name string, info *frac.Info) *frac.Sealed {
	return frac.NewSealed(
		name,
		b.readLimiter,
		b.cacheProvider.CreateIndexCache(),
		b.cacheProvider.CreateDocBlockCache(),
		info,
		b.config,
	)
}

func (b *builder) NewSealedPreloaded(name string, preloaded *frac.PreloadedData) *frac.Sealed {
	return frac.NewSealedPreloaded(
		name,
		preloaded,
		b.readLimiter,
		b.cacheProvider.CreateIndexCache(),
		b.config,
	)
}

func (b *builder) Stop() {
	b.indexWorkers.Stop()
}

func (b *builder) newActiveRef(active *frac.Active) activeRef {
	frac := &transitionFrac{active: active, builder: b}
	return activeRef{
		frac: frac,
		ref:  &fracRef{instance: frac},
	}
}
