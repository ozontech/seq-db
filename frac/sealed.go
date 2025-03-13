package frac

import (
	"context"
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
	Base

	docsReader  *disk.DocsReader
	indexReader *disk.IndexReader

	idsTable      IDsTable
	lidsTable     *lids.Table
	BlocksOffsets []uint64

	loadMu   *sync.RWMutex
	isLoaded bool

	readLimiter *disk.ReadLimiter
	indexCache  *IndexCache
	docsCache   *cache.Cache[[]byte]

	// shit for testing
	PartialSuicideMode PSD
}

type PSD int // emulates hard shutdown on different stages of fraction deletion, used for tests

const (
	Off PSD = iota
	HalfRename
	HalfRemove
)

func NewSealed(baseFile string, readLimiter *disk.ReadLimiter, indexCache *IndexCache, docsCache *cache.Cache[[]byte], fracInfoCache *Info) *Sealed {
	f := &Sealed{
		loadMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		Base: Base{
			info:         fracInfoCache,
			BaseFileName: baseFile,
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
		name := f.BaseFileName + consts.IndexFileSuffix
		indexFile, err := os.Open(name)
		if err != nil {
			logger.Fatal("can't open index file", zap.String("file", name), zap.Error(err))
		}
		f.indexReader = disk.NewIndexReader(f.readLimiter, indexFile, f.indexCache.Registry)
	}
}

func (f *Sealed) openDocs() {
	if f.docsReader == nil {
		name := f.BaseFileName + consts.DocsFileSuffix
		docsFile, err := os.Open(name)
		if err != nil {
			logger.Fatal("can't open docs file", zap.String("file", name), zap.Error(err))
		}
		f.docsReader = disk.NewDocsReader(f.readLimiter, docsFile, f.docsCache)
	}
}

func NewSealedFromActive(active *Active, readLimiter *disk.ReadLimiter, indexFile *os.File, indexCache *IndexCache) *Sealed {
	infoCopy := *active.info
	f := &Sealed{
		idsTable:      active.idsTable,
		lidsTable:     active.lidsTable,
		BlocksOffsets: active.DocBlocks.GetVals(),

		docsReader:  active.docsReader,
		indexReader: disk.NewIndexReader(readLimiter, indexFile, indexCache.Registry),

		loadMu:   &sync.RWMutex{},
		isLoaded: true,

		readLimiter: readLimiter,
		indexCache:  indexCache,
		docsCache:   active.docsReader.Cache,

		Base: Base{
			info:         &infoCopy,
			BaseFileName: active.BaseFileName,
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
		logger.Fatal("seq-db index file header corrupted", zap.String("file", f.indexReader.File.Name()))
	}
	info := &Info{}
	info.Load(block[4:])

	stat, err := f.indexReader.File.Stat()
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", f.indexReader.File.Name()), zap.Error(err))
	}

	info.MetaOnDisk = 0        // todo: make this correction on sealing
	info.Path = f.BaseFileName // todo: make this correction on sealing
	info.IndexOnDisk = uint64(stat.Size())

	return info
}

func (*Sealed) Type() string {
	return TypeSealed
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
	err := os.Rename(oldPath, newPath)
	if err != nil {
		logger.Error("can't rename docs file",
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
	err = os.Rename(oldPath, newPath)
	if err != nil {
		logger.Error("can't rename index file",
			zap.String("old_path", oldPath),
			zap.String("new_path", newPath),
			zap.Error(err),
		)
	}

	rmPath := f.BaseFileName + consts.DocsDelFileSuffix
	err = os.Remove(rmPath)
	if err != nil {
		logger.Error("can't remove docs file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRemove {
		return
	}

	rmPath = f.BaseFileName + consts.IndexDelFileSuffix
	err = os.Remove(rmPath)
	if err != nil {
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

	if f.docsReader.File != nil { // docs file may not be opened since it's loaded lazily
		if err := f.docsReader.File.Close(); err != nil {
			logger.Error("can't close docs file", f.closeLogArgs("sealed", hint, err)...)
		}
	}

	if err := f.indexReader.File.Close(); err != nil {
		logger.Error("can't close index file", f.closeLogArgs("sealed", hint, err)...)
	}
}

func (f *Sealed) FullSize() uint64 {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()
	return f.info.DocsOnDisk + f.info.IndexOnDisk
}

func (f *Sealed) String() string {
	return f.toString("sealed")
}

func (f *Sealed) TakeIndexes(ctx context.Context) (IndexProvider, func()) {
	f.useLock.RLock()

	if f.suicided {
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
		f.useLock.RUnlock()
		return EmptyIndexProvider{}, func() {}
	}

	defer func() {
		if panicData := recover(); panicData != nil {
			f.useLock.RUnlock()
			panic(panicData)
		}
	}()

	f.load()

	sc := NewSearchCell(ctx)
	ip := SealedIndexProvider{
		f:           f,
		sc:          sc,
		fracVersion: f.info.BinaryDataVer,
		midCache:    NewUnpackCache(),
		ridCache:    NewUnpackCache(),
	}

	return &ip, func() {
		ip.midCache.Release()
		ip.ridCache.Release()
		f.useLock.RUnlock()
	}
}
