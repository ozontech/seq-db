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

var (
	_ Fraction = (*Remote)(nil)
)

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

	// Per-section index files and their readers.
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

	loadMu     *sync.RWMutex
	isLoaded   bool
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
) *Remote {
	f := &Remote{
		ctx: ctx,

		loadMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		info:         info,
		BaseFileName: baseFile,
		Config:       config,

		s3cli: s3cli,

		skipMaskProvider: skipMaskProvider,
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

	if err := f.openInfoFile(); err != nil {
		logger.Error(
			"cannot open info file: any subsequent operation will fail",
			zap.String("fraction", filepath.Base(f.BaseFileName)),
			zap.Error(err),
		)
	}

	f.info = loadHeader(f.infoReader)
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
	if err := f.load(); err != nil {
		logger.Error(
			"will create empty data provider: cannot load remote fraction",
			zap.String("fraction", f.Info().Name()),
			zap.Error(err),
		)
		return nil, err
	}
	return &sealedDataProvider{
		ctx:               ctx,
		fractionTypeLabel: "remote",

		info:             f.info,
		config:           f.Config,
		docsReader:       &f.docsReader,
		blocksOffsets:    f.blocksData.BlocksOffsets,
		lidsTable:        f.blocksData.LIDsTable,
		lidsLoader:       lids.NewLoader(&f.lidReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, &f.tokenReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, &f.tokenReader, f.indexCache.TokenTable),

		idsTable: &f.blocksData.IDsTable,
		idsProvider: seqids.NewProvider(
			&f.idReader,
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
	util.MustRemoveFileByPath(f.BaseFileName + consts.RemoteFractionSuffix)

	f.docsCache.Release()
	f.indexCache.Release()

	files := []string{
		filepath.Base(f.BaseFileName) + consts.DocsFileSuffix,
		filepath.Base(f.BaseFileName) + consts.SdocsFileSuffix,
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

func (f *Remote) load() error {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if f.isLoaded {
		return nil
	}

	if err := f.openDocs(); err != nil {
		return err
	}

	if err := f.openIndexFiles(); err != nil {
		return err
	}

	readers := IndexReaders{
		Info:    f.infoReader,
		Token:   f.tokenReader,
		Offsets: f.offsetsReader,
		ID:      f.idReader,
		LID:     f.lidReader,
	}
	(&Loader{}).Load(&f.blocksData, f.info, readers)
	f.isLoaded = true

	return nil
}

func (f *Remote) openInfoFile() error {
	if f.infoFile != nil {
		return nil
	}
	return f.openRemoteFile(
		consts.InfoFileSuffix,
		func(file storage.ImmutableFile) {
			f.infoFile = file
			f.infoReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.InfoRegistry)
		},
	)
}

func (f *Remote) openIndexFiles() error {
	if err := f.openInfoFile(); err != nil {
		return err
	}
	if f.tokenFile == nil {
		if err := f.openRemoteFile(consts.TokenFileSuffix, func(file storage.ImmutableFile) {
			f.tokenFile = file
			f.tokenReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.TokenRegistry)
		}); err != nil {
			return err
		}
	}
	if f.offsetsFile == nil {
		if err := f.openRemoteFile(consts.OffsetsFileSuffix, func(file storage.ImmutableFile) {
			f.offsetsFile = file
			f.offsetsReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.OffsetsRegistry)
		}); err != nil {
			return err
		}
	}
	if f.idFile == nil {
		if err := f.openRemoteFile(consts.IDFileSuffix, func(file storage.ImmutableFile) {
			f.idFile = file
			f.idReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.IDRegistry)
		}); err != nil {
			return err
		}
	}
	if f.lidFile == nil {
		if err := f.openRemoteFile(consts.LIDFileSuffix, func(file storage.ImmutableFile) {
			f.lidFile = file
			f.lidReader = storage.NewIndexReader(f.readLimiter, file.Name(), file, f.indexCache.LIDRegistry)
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
		return fmt.Errorf("cannot check existence of %q file: %w", suffix, err)
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
