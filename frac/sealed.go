package frac

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

var _ Fraction = (*Sealed)(nil)

type Sealed struct {
	Config *Config

	BaseFileName string

	info *common.Info

	docsFile   *os.File
	docsCache  *cache.Cache[[]byte]
	docsReader storage.DocsReader

	// IsLegacy is true for fractions that use the old single .index file format.
	IsLegacy     bool
	legacyFile   *os.File
	legacyReader storage.IndexReader

	// Per-section index files and their readers (new split format only).
	infoFile    *os.File
	tokenFile   *os.File
	offsetsFile *os.File
	idFile      *os.File
	lidFile     *os.File

	tokenReader   storage.IndexReader
	offsetsReader storage.IndexReader
	idReader      storage.IndexReader
	lidReader     storage.IndexReader

	blocksData sealed.BlocksData
	indexCache *IndexCache

	initMu   *sync.RWMutex
	isInited bool

	readLimiter *storage.ReadLimiter

	// shit for testing
	PartialSuicideMode PSD

	skipMaskProvider skipMaskProvider
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
	info *common.Info,
	config *Config,
	skipMaskProvider skipMaskProvider,
	isLegacy bool,
) *Sealed {
	f := &Sealed{
		initMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		IsLegacy:     isLegacy,
		info:         info,
		BaseFileName: baseFile,
		Config:       config,

		PartialSuicideMode: Off,

		skipMaskProvider: skipMaskProvider,
	}

	// Fast path: if info cache has valid index size, skip opening the info file now.
	if info != nil && info.IndexOnDisk > 0 {
		return f
	}

	f.loadInfo()
	f.computeIndexSize()

	return f
}

func NewSealedPreloaded(
	baseFile string,
	preloaded *sealed.PreloadedData,
	rl *storage.ReadLimiter,
	indexCache *IndexCache,
	docsCache *cache.Cache[[]byte],
	config *Config,
	skipMaskProvider skipMaskProvider,
) *Sealed {
	f := &Sealed{
		blocksData: preloaded.BlocksData,
		docsCache:  docsCache,
		indexCache: indexCache,

		initMu:   &sync.RWMutex{},
		isInited: true,

		readLimiter: rl,

		info:         preloaded.Info,
		BaseFileName: baseFile,
		Config:       config,

		skipMaskProvider: skipMaskProvider,
	}

	// Put token table built during sealing into the cache.
	indexCache.TokenTable.Get(token.CacheKeyTable, func() (token.Table, int) {
		return preloaded.TokenTable, preloaded.TokenTable.Size()
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

func (f *Sealed) openInfoLegacy() {
	if f.legacyFile != nil {
		return
	}

	name := f.BaseFileName + consts.IndexFileSuffix
	file, err := os.Open(name)
	if err != nil {
		logger.Fatal(
			"can't open legacy index file",
			zap.String("file", name),
			zap.Error(err),
		)
	}

	f.legacyFile = file
	f.legacyReader = storage.NewIndexReader(
		f.readLimiter, file.Name(),
		file, f.indexCache.LegacyRegistry,
	)
}

func (f *Sealed) openInfo() {
	if f.infoFile != nil {
		return
	}

	name := f.BaseFileName + consts.InfoFileSuffix
	file, err := os.Open(name)
	if err != nil {
		logger.Fatal(
			"can't open info file",
			zap.String("file", name),
			zap.Error(err),
		)
	}

	f.infoFile = file
}

func (f *Sealed) openIndex() {
	if f.IsLegacy {
		// We have exactly one `.index` file for legacy sealed fractions.
		// So opening only this file is sufficient.
		f.openInfoLegacy()
		return
	}

	f.openInfo()
	if f.tokenFile == nil {
		name := f.BaseFileName + consts.TokenFileSuffix
		file, err := os.Open(name)
		if err != nil {
			logger.Fatal("can't open token file", zap.String("file", name), zap.Error(err))
		}
		f.tokenFile = file
		f.tokenReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.TokenRegistry)
	}

	if f.offsetsFile == nil {
		name := f.BaseFileName + consts.OffsetsFileSuffix
		file, err := os.Open(name)
		if err != nil {
			logger.Fatal("can't open offsets file", zap.String("file", name), zap.Error(err))
		}
		f.offsetsFile = file
		f.offsetsReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.OffsetsRegistry)
	}

	if f.idFile == nil {
		name := f.BaseFileName + consts.IDFileSuffix
		file, err := os.Open(name)
		if err != nil {
			logger.Fatal("can't open id file", zap.String("file", name), zap.Error(err))
		}
		f.idFile = file
		f.idReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.IDRegistry)
	}

	if f.lidFile == nil {
		name := f.BaseFileName + consts.LIDFileSuffix
		file, err := os.Open(name)
		if err != nil {
			logger.Fatal("can't open lid file", zap.String("file", name), zap.Error(err))
		}
		f.lidFile = file
		f.lidReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.LIDRegistry)
	}
}

func (f *Sealed) openDocs() {
	if f.docsFile != nil {
		return
	}

	var err error
	f.docsFile, err = os.Open(f.BaseFileName + consts.SdocsFileSuffix)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Fatal(
				"can't open sdocs file",
				zap.String("frac", f.BaseFileName),
				zap.Error(err),
			)
		}

		f.docsFile, err = os.Open(f.BaseFileName + consts.DocsFileSuffix)
		if err != nil {
			logger.Fatal(
				"can't open docs file",
				zap.String("frac", f.BaseFileName),
				zap.Error(err),
			)
		}
	}

	f.docsReader = storage.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
}

func (f *Sealed) loadInfo() {
	var err error

	if f.IsLegacy {
		f.openInfoLegacy()
		if f.info, err = loadInfoLegacy(f.legacyReader); err != nil {
			logger.Fatal("error loading Info", zap.String("fraction", f.BaseFileName), zap.Error(err))
		}
		return
	}

	f.openInfo()
	if f.info, err = loadInfo(f.infoFile); err != nil {
		logger.Fatal("error loading Info", zap.String("fraction", f.BaseFileName), zap.Error(err))
	}
}

func (f *Sealed) init(full bool) {
	f.initMu.Lock()
	defer f.initMu.Unlock()

	f.openDocs()
	f.openIndex()

	if f.isInited || !full {
		return
	}

	if f.IsLegacy {
		(&LegacyLoader{}).Load(&f.blocksData, f.info, f.legacyReader)
		f.isInited = true
		return
	}

	(&Loader{}).Load(&f.blocksData, f.info, IndexReaders{
		Token:   f.tokenReader,
		Offsets: f.offsetsReader,
		ID:      f.idReader,
		LID:     f.lidReader,
	})

	f.isInited = true
}

// Offload saves all index files and docs to remote storage.
func (f *Sealed) Offload(ctx context.Context, u storage.Uploader) (bool, error) {
	f.init(false)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return u.Upload(gctx, f.docsFile) })

	if f.IsLegacy {
		g.Go(func() error { return u.Upload(gctx, f.legacyFile) })
	} else {
		g.Go(func() error { return u.Upload(gctx, f.infoFile) })
		g.Go(func() error { return u.Upload(gctx, f.tokenFile) })
		g.Go(func() error { return u.Upload(gctx, f.offsetsFile) })
		g.Go(func() error { return u.Upload(gctx, f.idFile) })
		g.Go(func() error { return u.Upload(gctx, f.lidFile) })
	}

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

func (f *Sealed) Release() {
	f.init(false)

	indexFiles := []*os.File{
		f.docsFile,
		f.infoFile,
		f.tokenFile,
		f.offsetsFile,
		f.idFile,
		f.lidFile,
	}

	if f.IsLegacy {
		indexFiles = []*os.File{
			f.docsFile,
			f.legacyFile,
		}
	}

	for _, file := range indexFiles {
		if file != nil {
			if err := file.Close(); err != nil {
				logger.Error(
					"can't close file",
					zap.String("file", file.Name()),
					zap.Error(err),
				)
			}
		}
	}

	f.docsCache.Release()
	f.indexCache.Release()
}

func (f *Sealed) Suicide() {
	f.Release()

	// Rename docs atomically first — this commits the intent to delete.
	oldPath := f.BaseFileName + consts.DocsFileSuffix
	newPath := f.BaseFileName + consts.DocsDelFileSuffix
	if err := os.Rename(oldPath, newPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(
			"can't rename docs file",
			zap.String("old", oldPath),
			zap.String("new", newPath),
			zap.Error(err),
		)
	}

	oldPath = f.BaseFileName + consts.SdocsFileSuffix
	newPath = f.BaseFileName + consts.SdocsDelFileSuffix
	if err := os.Rename(oldPath, newPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(
			"can't rename sdocs file",
			zap.String("old", oldPath),
			zap.String("new", newPath),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRename {
		return
	}

	// Delete all index files directly (they are regenerable; no atomic rename needed).
	indexSuffixes := []string{
		consts.InfoFileSuffix,
		consts.TokenFileSuffix,
		consts.OffsetsFileSuffix,
		consts.IDFileSuffix,
		consts.LIDFileSuffix,
	}

	if f.IsLegacy {
		indexSuffixes = []string{
			consts.IndexFileSuffix,
		}
	}

	for _, suffix := range indexSuffixes {
		if err := os.Remove(f.BaseFileName + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			logger.Error(
				"can't remove index file",
				zap.String("file", f.BaseFileName+suffix),
				zap.Error(err),
			)
		}
	}

	if err := os.Remove(f.BaseFileName + consts.DocsDelFileSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(
			"can't remove docs del file",
			zap.String("frac", f.BaseFileName),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRemove {
		return
	}

	if err := os.Remove(f.BaseFileName + consts.SdocsDelFileSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(
			"can't remove sdocs del file",
			zap.String("frac", f.BaseFileName),
			zap.Error(err),
		)
	}

	f.skipMaskProvider.RemoveFrac(f.info.Name())
}

func (f *Sealed) String() string {
	return fracToString(f, "sealed")
}

func (f *Sealed) Fetch(ctx context.Context, ids []seq.ID, noSkipMasks bool) ([][]byte, error) {
	dp := f.createDataProvider(ctx)
	defer dp.release()
	return dp.Fetch(ids, noSkipMasks)
}

func (f *Sealed) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	dp := f.createDataProvider(ctx)
	defer dp.release()
	return dp.Search(params)
}

func (f *Sealed) FindLIDs(ctx context.Context, ids []seq.ID) ([]seq.LID, error) {
	dp := f.createDataProvider(ctx)
	defer dp.release()

	return dp.FindLIDs(ids)
}

func (f *Sealed) createDataProvider(ctx context.Context) *sealedDataProvider {
	f.init(true)

	tokenReader := &f.tokenReader
	lidReader := &f.lidReader
	idReader := &f.idReader

	if f.IsLegacy {
		tokenReader = &f.legacyReader
		lidReader = &f.legacyReader
		idReader = &f.legacyReader
	}

	return &sealedDataProvider{
		ctx:               ctx,
		fractionTypeLabel: "sealed",

		info:             f.info,
		config:           f.Config,
		docsReader:       &f.docsReader,
		blocksOffsets:    f.blocksData.BlocksOffsets,
		lidsTable:        f.blocksData.LIDsTable,
		lidsLoader:       lids.NewLoader(f.info.BinaryDataVer, lidReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.info.BinaryDataVer, tokenReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.IsLegacy, tokenReader, f.indexCache.TokenTable),

		idsTable: &f.blocksData.IDsTable,
		idsProvider: seqids.NewProvider(
			idReader,
			f.indexCache.MIDs,
			f.indexCache.RIDs,
			f.indexCache.Params,
			&f.blocksData.IDsTable,
			f.info.BinaryDataVer,
		),

		skipMaskProvider: f.skipMaskProvider,
	}
}

func (f *Sealed) Info() *common.Info {
	return f.info
}

func (f *Sealed) Contains(id seq.MID) bool {
	return f.info.IsIntersecting(id, id)
}

func (f *Sealed) IsIntersecting(from, to seq.MID) bool {
	return f.info.IsIntersecting(from, to)
}

func loadInfoLegacy(infoReader storage.IndexReader) (*common.Info, error) {
	block, _, err := infoReader.ReadIndexBlock(0, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot read info block: %w", err)
	}

	var bi sealed.BlockInfo
	if err := bi.Unpack(block); err != nil {
		return nil, fmt.Errorf("cannot unpack info block: %w", err)
	}

	return bi.Info, nil
}

func loadInfo(r interface {
	io.ReaderAt
	Stat() (os.FileInfo, error)
},
) (*common.Info, error) {
	stat, err := r.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot stat info file: %w", err)
	}

	block := make([]byte, stat.Size())
	if _, err := r.ReadAt(block, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cannot read info block: %w", err)
	}

	var bi sealed.BlockInfo
	if err := bi.Unpack(block); err != nil {
		return nil, fmt.Errorf("cannot unpack info block: %w", err)
	}

	return bi.Info, nil
}

// computeIndexOnDisk returns the total on-disk size of index files for a local fraction.
func (f *Sealed) computeIndexSize() {
	suffixes := []string{
		consts.InfoFileSuffix,
		consts.TokenFileSuffix,
		consts.OffsetsFileSuffix,
		consts.IDFileSuffix,
		consts.LIDFileSuffix,
	}

	if f.IsLegacy {
		suffixes = []string{
			consts.IndexFileSuffix,
		}
	}

	f.info.IndexOnDisk = 0
	for _, suffix := range suffixes {
		st, err := os.Stat(f.info.Path + suffix)
		if err != nil {
			logger.Fatal(
				"can't stat index file",
				zap.String("file", f.info.Path+suffix),
				zap.Error(err),
			)
		}

		f.info.IndexOnDisk += uint64(st.Size())
	}
}
