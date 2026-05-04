package frac

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"go.uber.org/zap"

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
	"github.com/ozontech/seq-db/storage/s3"
	"github.com/ozontech/seq-db/util"
)

var _ Fraction = (*Remote)(nil)

// Remote fraction is a fraction that is backed by remote storage.
//
// Structure of [Remote] fraction is almost identical to the [Sealed] one.
// In fact, they share the same on-disk binary layout, access methods and any other logic,
// but having [Remote] fraction allows us to easily distinguish between local and remote fractions.
type Remote struct {
	ctx context.Context

	Config *Config

	BaseFileName string

	info *common.Info

	docsFile   storage.ImmutableFile
	docsCache  *cache.Cache[[]byte]
	docsReader storage.DocsReader

	// IsLegacy is true for fractions that use the old single .index file format.
	IsLegacy     bool
	legacyFile   storage.ImmutableFile
	legacyReader storage.IndexReader

	// Per-section index files and their readers (new split format only).
	infoFile    storage.ImmutableFile
	tokenFile   storage.ImmutableFile
	offsetsFile storage.ImmutableFile
	idFile      storage.ImmutableFile
	lidFile     storage.ImmutableFile

	infoReader    storage.IndexReader
	tokenReader   storage.IndexReader
	offsetsReader storage.IndexReader
	idReader      storage.IndexReader
	lidReader     storage.IndexReader

	indexCache *IndexCache

	initMu     *sync.RWMutex
	isInited   bool
	blocksData sealed.BlocksData

	s3cli       *s3.Client
	readLimiter *storage.ReadLimiter

	skipMaskProvider skipMaskProvider
}

func NewRemote(
	ctx context.Context,
	baseFile string,
	readLimiter *storage.ReadLimiter,
	indexCache *IndexCache,
	docsCache *cache.Cache[[]byte],
	info *common.Info,
	config *Config,
	s3cli *s3.Client,
	skipMaskProvider skipMaskProvider,
	isLegacy bool,
) *Remote {
	f := &Remote{
		ctx: ctx,

		initMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		info:         info,
		BaseFileName: baseFile,
		Config:       config,

		s3cli:            s3cli,
		skipMaskProvider: skipMaskProvider,

		IsLegacy: isLegacy,
	}

	// Fast path if fraction-info cache exists AND it has valid index size.
	//
	// Usually it means that this fraction was created by [fracmanager.FracManager] after offloading
	// and info is already present. Or fraction's info was persisted in `.frac-cache`.
	if info != nil && info.IndexOnDisk > 0 {
		return f
	}

	// FIXME(dkharms): For now almost any availability issues with S3 will cause seq-db to panic during initialisation phase.
	// I wrote a small proposal on how we can reduce impact of such events.
	// https://github.com/ozontech/seq-db/issues/92

	if err := f.loadInfo(); err != nil {
		logger.Error(
			"cannot open info file: any subsequent operation will fail",
			zap.String("fraction", filepath.Base(f.BaseFileName)),
			zap.Error(err),
		)
	}

	return f
}

func (f *Remote) Contains(mid seq.MID) bool {
	return f.info.IsIntersecting(mid, mid)
}

func (f *Remote) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	dp, err := f.createDataProvider(ctx)
	if err != nil {
		return nil, err
	}
	defer dp.release()

	return dp.Fetch(ids)
}

func (f *Remote) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	dp, err := f.createDataProvider(ctx)
	if err != nil {
		return &seq.QPR{Aggs: make([]seq.AggregatableSamples, len(params.AggQ))}, err
	}
	defer dp.release()

	return dp.Search(params)
}

func (f *Remote) FindLIDs(ctx context.Context, ids []seq.ID) ([]seq.LID, error) {
	dp, err := f.createDataProvider(ctx)
	if err != nil {
		return nil, err
	}
	defer dp.release()

	return dp.FindLIDs(ids)
}

func (f *Remote) createDataProvider(ctx context.Context) (*sealedDataProvider, error) {
	if err := f.init(); err != nil {
		logger.Error(
			"will create empty data provider: cannot load remote fraction",
			zap.String("fraction", f.Info().Name()),
			zap.Error(err),
		)
		return nil, err
	}

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
		fractionTypeLabel: "remote",

		info:             f.info,
		config:           f.Config,
		docsReader:       &f.docsReader,
		blocksOffsets:    f.blocksData.BlocksOffsets,
		lidsTable:        f.blocksData.LIDsTable,
		lidsLoader:       lids.NewLoader(lidReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, tokenReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, tokenReader, f.indexCache.TokenTable),

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
	}, nil
}

func (f *Remote) Info() *common.Info {
	return f.info
}

func (f *Remote) IsIntersecting(from, to seq.MID) bool {
	return f.info.IsIntersecting(from, to)
}

func (f *Remote) Suicide() {
	// FIXME(dkharms): We need to rename `.remote` file to `._remote` to commit deletion intent.
	// Now, we might have fraction leaks in S3 storage since [Suicide] is not atomic.
	util.MustRemoveFileByPath(f.BaseFileName + consts.RemoteFractionSuffix)

	f.docsCache.Release()
	f.indexCache.Release()

	files := []string{
		filepath.Base(f.BaseFileName) + consts.DocsFileSuffix,
		filepath.Base(f.BaseFileName) + consts.SdocsFileSuffix,
		// Legacy single-file format.
		filepath.Base(f.BaseFileName) + consts.IndexFileSuffix,
		// New split format.
		filepath.Base(f.BaseFileName) + consts.InfoFileSuffix,
		filepath.Base(f.BaseFileName) + consts.TokenFileSuffix,
		filepath.Base(f.BaseFileName) + consts.OffsetsFileSuffix,
		filepath.Base(f.BaseFileName) + consts.IDFileSuffix,
		filepath.Base(f.BaseFileName) + consts.LIDFileSuffix,
	}

	err := f.s3cli.Remove(f.ctx, files...)
	if err != nil {
		logger.Info(
			"failed to delete files during suicide",
			zap.Any("files", files),
			zap.Error(err),
		)
	}

	f.skipMaskProvider.RemoveFrac(f.info.Name())
}

func (f *Remote) String() string {
	return fracToString(f, "remote")
}

func (f *Remote) loadInfo() error {
	if f.IsLegacy {
		if err := f.openInfoLegacy(); err != nil {
			return err
		}
		f.info = loadInfo(f.legacyReader)

		return nil
	}

	if err := f.openInfo(); err != nil {
		return err
	}
	f.info = loadInfo(f.infoReader)

	return nil
}

func (f *Remote) init() error {
	f.initMu.Lock()
	defer f.initMu.Unlock()

	if err := f.openDocs(); err != nil {
		return err
	}

	if err := f.openIndex(); err != nil {
		return err
	}

	if f.isInited {
		return nil
	}

	if f.IsLegacy {
		(&LegacyLoader{}).Load(&f.blocksData, f.info, f.legacyReader)
		f.isInited = true
		return nil
	}

	(&Loader{}).Load(&f.blocksData, f.info, IndexReaders{
		Info:    f.infoReader,
		Token:   f.tokenReader,
		Offsets: f.offsetsReader,
		ID:      f.idReader,
		LID:     f.lidReader,
	})

	f.isInited = true
	return nil
}

func (f *Remote) openInfoLegacy() error {
	if f.legacyFile != nil {
		return nil
	}

	return f.openRemoteFile(consts.IndexFileSuffix, func(file storage.ImmutableFile) {
		f.legacyFile = file
		f.legacyReader = storage.NewIndexReader(
			f.readLimiter, file.Name(),
			file, f.indexCache.LegacyRegistry,
		)
	})
}

func (f *Remote) openInfo() error {
	if f.infoFile != nil {
		return nil
	}

	return f.openRemoteFile(consts.InfoFileSuffix, func(file storage.ImmutableFile) {
		f.infoFile = file
		f.infoReader = storage.NewIndexReader(
			f.readLimiter, file.Name(),
			file, f.indexCache.InfoRegistry,
		)
	})
}

func (f *Remote) openIndex() error {
	if err := f.openInfo(); err != nil {
		return err
	}

	if f.IsLegacy {
		return nil
	}

	if f.tokenFile == nil {
		if err := f.openRemoteFile(consts.TokenFileSuffix, func(file storage.ImmutableFile) {
			f.tokenFile = file
			f.tokenReader = storage.NewIndexReader(
				f.readLimiter, file.Name(),
				file, f.indexCache.TokenRegistry,
			)
		}); err != nil {
			return err
		}
	}

	if f.offsetsFile == nil {
		if err := f.openRemoteFile(consts.OffsetsFileSuffix, func(file storage.ImmutableFile) {
			f.offsetsFile = file
			f.offsetsReader = storage.NewIndexReader(
				f.readLimiter, file.Name(),
				file, f.indexCache.OffsetsRegistry,
			)
		}); err != nil {
			return err
		}
	}

	if f.idFile == nil {
		if err := f.openRemoteFile(consts.IDFileSuffix, func(file storage.ImmutableFile) {
			f.idFile = file
			f.idReader = storage.NewIndexReader(
				f.readLimiter, file.Name(),
				file, f.indexCache.IDRegistry,
			)
		}); err != nil {
			return err
		}
	}

	if f.lidFile == nil {
		if err := f.openRemoteFile(consts.LIDFileSuffix, func(file storage.ImmutableFile) {
			f.lidFile = file
			f.lidReader = storage.NewIndexReader(
				f.readLimiter, file.Name(),
				file, f.indexCache.LIDRegistry,
			)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (f *Remote) openRemoteFile(suffix string, assign func(storage.ImmutableFile)) error {
	name := filepath.Base(f.BaseFileName) + suffix

	ok, err := f.s3cli.Exists(f.ctx, name)
	if err != nil {
		return fmt.Errorf(
			"cannot check existence of %q file: %w",
			suffix, err,
		)
	}

	if !ok {
		return fmt.Errorf("missing %q file", suffix)
	}

	assign(s3.NewReader(f.ctx, f.s3cli, name))
	return nil
}

func (f *Remote) openDocs() error {
	if f.docsFile != nil {
		return nil
	}

	sortedName := filepath.Base(f.BaseFileName) + consts.SdocsFileSuffix
	unsortedName := filepath.Base(f.BaseFileName) + consts.DocsFileSuffix

	unsortedExists, err := f.s3cli.Exists(f.ctx, unsortedName)
	if err != nil {
		return fmt.Errorf(
			"cannot check existence of %q file: %w",
			consts.DocsFileSuffix, err,
		)
	}

	if unsortedExists {
		f.docsFile = s3.NewReader(f.ctx, f.s3cli, unsortedName)
		f.docsReader = storage.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
		return nil
	}

	sortedExists, err := f.s3cli.Exists(f.ctx, sortedName)
	if err != nil {
		return fmt.Errorf(
			"cannot check existence of %q file: %w",
			consts.SdocsFileSuffix, err,
		)
	}

	if sortedExists {
		f.docsFile = s3.NewReader(f.ctx, f.s3cli, sortedName)
		f.docsReader = storage.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
		return nil
	}

	return fmt.Errorf("missing %q and %q files", consts.DocsFileSuffix, consts.SdocsFileSuffix)
}
