package frac

import (
	"context"
	"errors"
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/util"
)

const seqDBMagic = "SEQM"

type Sealed struct {
	frac

	docsFile   *os.File
	docsCache  *cache.Cache[[]byte]
	docsReader *disk.DocsReader

	indexFile   *os.File
	indexCache  *IndexCache
	indexReader *disk.IndexReader

	idsTable      IDsTable
	lidsTable     *lids.Table
	BlocksOffsets []uint64

	loadMu   *sync.RWMutex
	isLoaded bool

	readLimiter *disk.ReadLimiter

	// shit for testing
	PartialSuicideMode PSD
}

type PSD int // emulates hard shutdown on different stages of fraction deletion, used for tests

const (
	Off PSD = iota
	HalfRename
	HalfRemove
)

func NewSealed(
	baseFile string,
	readLimiter *disk.ReadLimiter,
	indexCache *IndexCache,
	docsCache *cache.Cache[[]byte],
	fracInfoCache *Info,
	config Config,
) *Sealed {
	f := &Sealed{
		loadMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		frac: frac{
			info:         fracInfoCache,
			BaseFileName: baseFile,
			Config:       config,
		},
		PartialSuicideMode: Off,
	}

	// fast path if fraction-info cache exists AND it has valid index size
	if fracInfoCache != nil && fracInfoCache.IndexOnDisk > 0 {
		return f
	}

	f.openIndex()
	f.info = f.loadHeader()

	return f
}

func (f *Sealed) openIndex() {
	if f.indexReader == nil {
		var err error
		name := f.BaseFileName + consts.IndexFileSuffix
		f.indexFile, err = os.Open(name)
		if err != nil {
			logger.Fatal("can't open index file", zap.String("file", name), zap.Error(err))
		}
		f.indexReader = disk.NewIndexReader(f.readLimiter, f.indexFile, f.indexCache.Registry)
	}
}

func (f *Sealed) openDocs() {
	if f.docsReader == nil {
		var err error
		f.docsFile, err = os.Open(f.BaseFileName + consts.DocsFileSuffix)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				logger.Fatal("can't open docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
			}
			f.docsFile, err = os.Open(f.BaseFileName + consts.SdocsFileSuffix)
			if err != nil {
				logger.Fatal("can't open sdocs file", zap.String("frac", f.BaseFileName), zap.Error(err))
			}
		}
		f.docsReader = disk.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
	}
}

func NewSealedFromActive(active *Active, readLimiter *disk.ReadLimiter, indexFile *os.File, indexCache *IndexCache) *Sealed {
	infoCopy := *active.info
	f := &Sealed{
		idsTable:      active.idsTable,
		lidsTable:     active.lidsTable,
		BlocksOffsets: active.DocBlocks.GetVals(),

		docsFile:   active.docsFile,
		docsCache:  active.docsCache,
		docsReader: active.docsReader,

		indexFile:   indexFile,
		indexCache:  indexCache,
		indexReader: disk.NewIndexReader(readLimiter, indexFile, indexCache.Registry),

		loadMu:   &sync.RWMutex{},
		isLoaded: true,

		readLimiter: readLimiter,

		frac: frac{
			info:         &infoCopy,
			BaseFileName: active.BaseFileName,
			Config:       active.Config,
		},
	}

	// put the token table built during sealing into the cache of the sealed faction
	indexCache.TokenTable.Get(token.CacheKeyTable, func() (token.Table, int) {
		return active.tokenTable, active.tokenTable.Size()
	})

	docsCountK := float64(f.info.DocsTotal) / 1000
	logger.Info("sealed fraction created from active",
		zap.String("frac", f.info.Name()),
		util.ZapMsTsAsESTimeStr("creation_time", f.info.CreationTime),
		zap.String("from", f.info.From.String()),
		zap.String("to", f.info.To.String()),
		util.ZapFloat64WithPrec("docs_k", docsCountK, 1),
	)

	f.info.MetaOnDisk = 0

	return f
}

func (f *Sealed) loadHeader() *Info {
	block, _, err := f.indexReader.ReadIndexBlock(0, nil)
	if err != nil {
		logger.Panic("todo")
	}
	if len(block) < 4 || string(block[:4]) != seqDBMagic {
		logger.Fatal("seq-db index file header corrupted", zap.String("file", f.indexFile.Name()))
	}
	info := &Info{}
	info.Load(block[4:])

	stat, err := f.indexFile.Stat()
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", f.indexFile.Name()), zap.Error(err))
	}

	info.MetaOnDisk = 0        // todo: make this correction on sealing
	info.Path = f.BaseFileName // todo: make this correction on sealing
	info.IndexOnDisk = uint64(stat.Size())

	return info
}

func (f *Sealed) load() {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {

		f.openDocs()
		f.openIndex()

		(&Loader{}).Load(f)
		f.isLoaded = true
	}
}

func (f *Sealed) Suicide() {
	f.useLock.Lock()
	f.suicided = true
	f.useLock.Unlock()

	f.close("suicide")

	f.docsCache.Release()
	f.indexCache.Release()

	// make some atomic magic, to be more stable on removing fractions
	oldPath := f.BaseFileName + consts.DocsFileSuffix
	newPath := f.BaseFileName + consts.DocsDelFileSuffix
	if err := os.Rename(oldPath, newPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("can't rename docs file",
			zap.String("old_path", oldPath),
			zap.String("new_path", newPath),
			zap.Error(err),
		)
	}

	oldPath = f.BaseFileName + consts.SdocsFileSuffix
	newPath = f.BaseFileName + consts.SdocsDelFileSuffix
	if err := os.Rename(oldPath, newPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("can't rename sdocs file",
			zap.String("old_path", oldPath),
			zap.String("new_path", newPath),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRename {
		return
	}

	oldPath = f.BaseFileName + consts.IndexFileSuffix
	newPath = f.BaseFileName + consts.IndexDelFileSuffix
	if err := os.Rename(oldPath, newPath); err != nil {
		logger.Error("can't rename index file",
			zap.String("old_path", oldPath),
			zap.String("new_path", newPath),
			zap.Error(err),
		)
	}

	rmPath := f.BaseFileName + consts.DocsDelFileSuffix
	if err := os.Remove(rmPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("can't remove docs file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}

	rmPath = f.BaseFileName + consts.SdocsDelFileSuffix
	if err := os.Remove(rmPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("can't remove sdocs file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRemove {
		return
	}

	rmPath = f.BaseFileName + consts.IndexDelFileSuffix
	if err := os.Remove(rmPath); err != nil {
		logger.Error("can't remove index file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}
}

func (f *Sealed) close(hint string) {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {
		return
	}

	if f.docsFile != nil { // docs file may not be opened since it's loaded lazily
		if err := f.docsFile.Close(); err != nil {
			logger.Error("can't close docs file", f.closeLogArgs("sealed", hint, err)...)
		}
	}

	if err := f.indexFile.Close(); err != nil {
		logger.Error("can't close index file", f.closeLogArgs("sealed", hint, err)...)
	}
}

func (f *Sealed) String() string {
	return f.toString("sealed")
}

func (f *Sealed) DataProvider(ctx context.Context) (DataProvider, func()) {
	f.useLock.RLock()

	if f.suicided {
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
		f.useLock.RUnlock()
		return EmptyDataProvider{}, func() {}
	}

	defer func() {
		if panicData := recover(); panicData != nil {
			f.useLock.RUnlock()
			panic(panicData)
		}
	}()

	f.load()

	dp := f.createDataProvider(ctx)

	return dp, func() {
		dp.release()
		f.useLock.RUnlock()
	}
}

func (f *Sealed) createDataProvider(ctx context.Context) *sealedDataProvider {
	return &sealedDataProvider{
		ctx:              ctx,
		info:             f.Info(),
		config:           &f.Config,
		docsReader:       f.docsReader,
		blocksOffsets:    f.BlocksOffsets,
		fracVersion:      f.info.BinaryDataVer,
		midCache:         NewUnpackCache(),
		ridCache:         NewUnpackCache(),
		lidsTable:        f.lidsTable,
		idsLoader:        NewIDsLoader(f.indexReader, f.indexCache, f.idsTable),
		lidsLoader:       lids.NewLoader(f.indexReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.indexReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.indexReader, f.indexCache.TokenTable),
	}
}
