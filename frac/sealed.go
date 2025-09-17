package frac

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

var (
	_ Fraction = (*Sealed)(nil)
)

type Sealed struct {
	Config *Config

	BaseFileName string

	info *Info

	useMu    sync.RWMutex
	suicided bool

	docsFile   *os.File
	docsCache  *cache.Cache[[]byte]
	docsReader storage.DocsReader

	indexFile   *os.File
	indexCache  *IndexCache
	indexReader storage.IndexReader

	loadMu   *sync.RWMutex
	isLoaded bool
	state    sealedState

	readLimiter *storage.ReadLimiter

	// shit for testing
	PartialSuicideMode PSD
}

type sealedState struct {
	idsTable      seqids.Table
	lidsTable     *lids.Table
	BlocksOffsets []uint64
}

type PSD int // emulates hard shutdown on different stages of fraction deletion, used for tests

const (
	Off PSD = iota
	HalfRename
	HalfRemove
)

func NewSealed(
	baseFile string,
	readLimiter *storage.ReadLimiter,
	indexCache *IndexCache,
	docsCache *cache.Cache[[]byte],
	info *Info,
	config *Config,
) *Sealed {
	f := &Sealed{
		loadMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		info:         info,
		BaseFileName: baseFile,
		Config:       config,

		PartialSuicideMode: Off,
	}

	// fast path if fraction-info cache exists AND it has valid index size
	if info != nil && info.IndexOnDisk > 0 {
		return f
	}

	f.openIndex()
	f.info = loadHeader(f.indexFile, f.indexReader)

	return f
}

func (f *Sealed) openIndex() {
	if f.indexFile == nil {
		var err error
		name := f.BaseFileName + consts.IndexFileSuffix
		f.indexFile, err = os.Open(name)
		if err != nil {
			logger.Fatal("can't open index file", zap.String("file", name), zap.Error(err))
		}
		f.indexReader = storage.NewIndexReader(f.readLimiter, f.indexFile.Name(), f.indexFile, f.indexCache.Registry)
	}
}

func (f *Sealed) openDocs() {
	if f.docsFile == nil {
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
		f.docsReader = storage.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
	}
}

type PreloadedData struct {
	info          *Info
	idsTable      seqids.Table
	lidsTable     *lids.Table
	tokenTable    token.Table
	blocksOffsets []uint64
	indexFile     *os.File
	docsFile      *os.File
}

func NewSealedPreloaded(
	baseFile string,
	preloaded *PreloadedData,
	rl *storage.ReadLimiter,
	indexCache *IndexCache,
	docsCache *cache.Cache[[]byte],
	config *Config,
) *Sealed {
	f := &Sealed{
		state: sealedState{
			idsTable:      preloaded.idsTable,
			lidsTable:     preloaded.lidsTable,
			BlocksOffsets: preloaded.blocksOffsets,
		},

		docsFile:   preloaded.docsFile,
		docsCache:  docsCache,
		docsReader: storage.NewDocsReader(rl, preloaded.docsFile, docsCache),

		indexFile:   preloaded.indexFile,
		indexCache:  indexCache,
		indexReader: storage.NewIndexReader(rl, preloaded.indexFile.Name(), preloaded.indexFile, indexCache.Registry),

		loadMu:   &sync.RWMutex{},
		isLoaded: true,

		readLimiter: rl,

		info:         preloaded.info,
		BaseFileName: baseFile,
		Config:       config,
	}

	// put the token table built during sealing into the cache of the sealed fraction
	indexCache.TokenTable.Get(token.CacheKeyTable, func() (token.Table, int) {
		return preloaded.tokenTable, preloaded.tokenTable.Size()
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

func (f *Sealed) load() {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {

		f.openDocs()
		f.openIndex()

		(&Loader{}).Load(&f.state, f.info, &f.indexReader)
		f.isLoaded = true
	}
}

// Offload saves `.docs` (or `.sdocs`) and `.index` files into remote storage.
// It does not free any of the occupied memory (nor on disk nor in memory).
func (f *Sealed) Offload(ctx context.Context, u storage.Uploader) (bool, error) {
	f.useMu.Lock()
	defer f.useMu.Unlock()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		f.openDocs()
		return u.Upload(gctx, f.docsFile)
	})

	g.Go(func() error {
		f.openIndex()
		return u.Upload(gctx, f.indexFile)
	})

	if err := g.Wait(); err != nil {
		return true, err
	}

	remoteFracName := f.BaseFileName + consts.RemoteFractionSuffix

	file, err := os.Create(remoteFracName)
	if err != nil {
		return true, err
	}
	defer file.Close()

	util.MustSyncPath(filepath.Dir(remoteFracName))
	return true, nil
}

func (f *Sealed) Suicide() {
	f.useMu.Lock()
	f.suicided = true
	f.useMu.Unlock()

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
			logger.Error("can't close docs file",
				zap.String("frac", f.BaseFileName),
				zap.String("type", "sealed"),
				zap.String("hint", hint),
				zap.Error(err))
		}
	}

	if err := f.indexFile.Close(); err != nil {
		logger.Error("can't close index file",
			zap.String("frac", f.BaseFileName),
			zap.String("type", "sealed"),
			zap.String("hint", hint),
			zap.Error(err))
	}
}

func (f *Sealed) String() string {
	return fracToString(f, "sealed")
}

func (f *Sealed) DataProvider(ctx context.Context) (DataProvider, func()) {
	f.useMu.RLock()

	if f.suicided {
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
		f.useMu.RUnlock()
		return EmptyDataProvider{}, func() {}
	}

	defer func() {
		if panicData := recover(); panicData != nil {
			f.useMu.RUnlock()
			panic(panicData)
		}
	}()

	f.load()

	dp := f.createDataProvider(ctx)

	return dp, func() {
		dp.release()
		f.useMu.RUnlock()
	}
}

func (f *Sealed) createDataProvider(ctx context.Context) *sealedDataProvider {
	return &sealedDataProvider{
		ctx:               ctx,
		fractionTypeLabel: "sealed",

		info:             f.info,
		config:           f.Config,
		docsReader:       &f.docsReader,
		blocksOffsets:    f.state.BlocksOffsets,
		lidsTable:        f.state.lidsTable,
		lidsLoader:       lids.NewLoader(&f.indexReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, &f.indexReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, &f.indexReader, f.indexCache.TokenTable),

		idsTable: &f.state.idsTable,
		idsProvider: seqids.NewProvider(
			&f.indexReader,
			f.indexCache.MIDs,
			f.indexCache.RIDs,
			f.indexCache.Params,
			&f.state.idsTable,
			f.info.BinaryDataVer,
		),
	}
}

func (f *Sealed) Info() *Info {
	return f.info
}

func (f *Sealed) Contains(id seq.MID) bool {
	return f.info.IsIntersecting(id, id)
}

func (f *Sealed) IsIntersecting(from, to seq.MID) bool {
	return f.info.IsIntersecting(from, to)
}

func loadHeader(
	indexFile storage.ImmutableFile,
	indexReader storage.IndexReader,
) *Info {
	block, _, err := indexReader.ReadIndexBlock(0, nil)
	if err != nil {
		logger.Fatal(
			"error reading info block from index",
			zap.String("file", indexFile.Name()),
			zap.Error(err),
		)
	}

	var bi BlockInfo
	if err := bi.Unpack(block); err != nil {
		logger.Fatal(
			"error unpacking info block",
			zap.String("file", indexFile.Name()),
			zap.Error(err),
		)
	}
	info := bi.Info

	// set index size
	stat, err := indexFile.Stat()
	if err != nil {
		logger.Fatal(
			"can't stat index file",
			zap.String("file", indexFile.Name()),
			zap.Error(err),
		)
	}

	info.IndexOnDisk = uint64(stat.Size())
	return info
}
