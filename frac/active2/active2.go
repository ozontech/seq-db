package active2

import (
	"context"
	"os"
	"sync"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"go.uber.org/zap"
)

type Conf struct {
	MetaRemove   bool
	IndexWorkers int
	SkipFsync    bool
	searchCfg    frac.SearchCfg
}

type Active2 struct {
	cfg *Conf

	mu      sync.RWMutex
	indexes []*Index
	info    *frac.Info
	sealed  *frac.Sealed

	docsFile *os.File
	metaFile *os.File

	docsReader *disk.DocsReader
	docsCache  *cache.Cache[[]byte]
}

func New(baseFileName string, readLimiter *disk.ReadLimiter, docsCache *cache.Cache[[]byte], cfg *Conf) *Active2 {
	docsFile, docsStats := openFile(baseFileName + consts.DocsFileSuffix)
	metaFile, metaStats := openFile(baseFileName + consts.MetaFileSuffix)

	f := &Active2{
		info:       frac.NewInfo(baseFileName, uint64(docsStats.Size()), uint64(metaStats.Size())),
		docsFile:   docsFile,
		metaFile:   metaFile,
		docsReader: disk.NewDocsReader(readLimiter, docsFile, docsCache),
		docsCache:  docsCache,
		cfg:        cfg,
	}

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	return f
}

func openFile(name string) (*os.File, os.FileInfo) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't create docs file", zap.String("file", name), zap.Error(err))
	}
	stat, err := file.Stat()
	if err != nil {
		logger.Fatal("can't stat docs file", zap.String("file", name), zap.Error(err))
	}
	return file, stat
}

func (a *Active2) IsIntersecting(from, to seq.MID) bool {
	return a.Info().IsIntersecting(from, to)
}

func (a *Active2) Contains(mid seq.MID) bool {
	return a.IsIntersecting(mid, mid)
}

func (a *Active2) Info() *frac.Info {
	a.mu.RLock()
	defer a.mu.RUnlock()

	info := *a.info

	return &info
}

func (a *Active2) DataProvider(ctx context.Context) (frac.DataProvider, func()) {
	a.mu.RLock()
	indexes := a.indexes
	sealed := a.sealed
	a.mu.RUnlock()

	if sealed != nil {
		metric.CountersTotal.WithLabelValues("use_sealed_from_active").Inc()
		return sealed.DataProvider(ctx)
	}

	if len(indexes) == 0 {
		return frac.EmptyIndexProvider{}, func() {}
	}

	return &DataProvider{ctx: ctx, indexes: indexes}, func() {}
}

func (a *Active2) Suicide() {

}
