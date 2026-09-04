package frac

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
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

	legacyFile           storage.ImmutableFile
	legacyReaderProvider *storage.ReaderProvider

	// Per-section index files and their readers (new split format only).
	infoFile    storage.ImmutableFile
	tokenFile   storage.ImmutableFile
	offsetsFile storage.ImmutableFile
	idFile      storage.ImmutableFile
	lidFile     storage.ImmutableFile

	tokenReaderProvider   *storage.ReaderProvider
	offsetsReaderProvider *storage.ReaderProvider
	idReaderProvider      *storage.ReaderProvider
	lidReaderProvider     *storage.ReaderProvider

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
	cfg *Config,
	s3cli *s3.Client,
	skipMaskProvider skipMaskProvider,
) *Remote {
	f := &Remote{
		ctx: ctx,

		initMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		info:         info,
		BaseFileName: baseFile,
		Config:       cfg,

		s3cli:            s3cli,
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

	if err := f.loadInfo(); err != nil {
		logger.Fatal(
			"cannot open info file: any subsequent operation will fail",
			zap.String("fraction", filepath.Base(f.BaseFileName)),
			zap.Error(err),
		)
	}
	f.computeIndexSize()

	return f
}

func (f *Remote) Contains(mid seq.MID) bool {
	return f.info.IsIntersecting(mid, mid)
}

func (f *Remote) Fetch(ctx context.Context, ids []seq.ID, noSkipMasks bool) ([][]byte, error) {
	dp, err := f.createDataProvider(ctx)
	if err != nil {
		return nil, err
	}
	defer dp.release()

	return dp.Fetch(ids, noSkipMasks)
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

func (f *Remote) mustGetReader(p *storage.ReaderProvider) storage.IndexReader {
	r, err := p.GetReader()
	if err != nil {
		logger.Fatal("error creating IndexReader", zap.String("fraction", f.BaseFileName), zap.Error(err))
	}
	return r
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

	var (
		tokenReader storage.IndexReader
		lidReader   storage.IndexReader
		idReader    storage.IndexReader
	)

	isLegacy := f.IsSingleIndex()
	if isLegacy {
		legacyReader := f.mustGetReader(f.legacyReaderProvider)
		tokenReader = legacyReader
		lidReader = legacyReader
		idReader = legacyReader
	} else {
		tokenReader = f.mustGetReader(f.tokenReaderProvider)
		lidReader = f.mustGetReader(f.lidReaderProvider)
		idReader = f.mustGetReader(f.idReaderProvider)
	}

	return &sealedDataProvider{
		ctx:               ctx,
		fractionTypeLabel: "remote",

		info:             f.info,
		config:           f.Config,
		docsReader:       &f.docsReader,
		blocksOffsets:    f.blocksData.BlocksOffsets,
		lidsTable:        f.blocksData.LIDsTable,
		lidsLoader:       lids.NewLoader(f.info.BinaryDataVer, &lidReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.info.BinaryDataVer, &tokenReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.info.BinaryDataVer, isLegacy, &tokenReader, f.indexCache.TokenTable),

		idsTable: &f.blocksData.IDsTable,
		idsProvider: seqids.NewProvider(
			&idReader,
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
	util.MustRemoveFileByPath(f.BaseFileName + consts.RemoteFractionInfoSuffix)

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

func (f *Remote) IsSingleIndex() bool {
	return f.info.BinaryDataVer < config.BinaryDataV3
}

// loadInfo loads the remote fraction information from available sources in priority order:
//  1. Local *.remote-info file (most up‑to‑date case).
//  2. Remote .info file on S3 (legacy but still supported).
//  3. Legacy *.index file on S3 (oldest scenario).
func (f *Remote) loadInfo() error {
	err := f.tryLoadInfoLocal()
	if err == nil {
		return nil
	}

	logger.Warn(
		"cannot open local info file for remote fraction, falling back to S3",
		zap.String("fraction", f.BaseFileName),
		zap.Error(err),
	)

	err = f.tryLoadInfoRemote()
	if err == nil {
		return nil
	}

	logger.Warn(
		"cannot open remote info file, falling back to legacy index",
		zap.String("fraction", f.BaseFileName),
		zap.Error(err),
	)

	return f.loadInfoLegacy()
}

// tryLoadInfoLocal attempts to load fraction information from a local file
// with the suffix .remote-info. This is the most preferred and modern approach,
// where all data is already present on disk.
func (f *Remote) tryLoadInfoLocal() error {
	remoteInfoPath := f.BaseFileName + consts.RemoteFractionInfoSuffix
	file, err := os.Open(remoteInfoPath)
	if err == nil {
		defer file.Close()
		f.info, err = loadInfo(file)
	}
	return err
}

// tryLoadInfoRemote attempts to load fraction information from a remote .info file
// located on S3. This is an intermediate fallback: it is used when the local
// .remote-info is absent, but an .info file still exists on S3 (maintained for
// backward compatibility).
func (f *Remote) tryLoadInfoRemote() error {
	err := f.openInfoRemote()
	if err == nil {
		f.info, err = loadInfo(f.infoFile)
	}
	return err
}

// loadInfoLegacy loads fraction information from the legacy index stored on S3.
// This is the oldest fallback, used when only an empty *.remote file exists locally
// and a single *.index file resides on S3 containing all necessary data.
func (f *Remote) loadInfoLegacy() error {
	err := f.openIndexLegacyRemote()
	if err == nil {
		f.info, err = loadInfoLegacy(f.mustGetReader(f.legacyReaderProvider))
	}
	return err
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

	if f.IsSingleIndex() {
		(&LegacyLoader{}).Load(&f.blocksData, f.info, f.mustGetReader(f.legacyReaderProvider))
		f.isInited = true
		return nil
	}

	(&Loader{}).Load(&f.blocksData, f.info, IndexReaders{
		Token:   f.mustGetReader(f.tokenReaderProvider),
		Offsets: f.mustGetReader(f.offsetsReaderProvider),
		ID:      f.mustGetReader(f.idReaderProvider),
		LID:     f.mustGetReader(f.lidReaderProvider),
	})

	f.isInited = true
	return nil
}

func (f *Remote) openIndexLegacyRemote() error {
	if f.legacyFile != nil {
		return nil
	}
	return f.openRemoteFile(consts.IndexFileSuffix, func(file storage.ImmutableFile) {
		f.legacyFile = file
		f.legacyReaderProvider = storage.NewReaderProvider(
			f.readLimiter, file.Name(),
			file, f.indexCache.LegacyRegistry,
		)
	})
}

func (f *Remote) openInfoRemote() error {
	if f.infoFile != nil {
		return nil
	}
	return f.openRemoteFile(consts.InfoFileSuffix, func(file storage.ImmutableFile) {
		f.infoFile = file
	})
}

func (f *Remote) openIndex() error {
	if f.IsSingleIndex() {
		return f.openIndexLegacyRemote()
	}

	if err := f.openInfoRemote(); err != nil {
		return err
	}

	if f.tokenFile == nil {
		err := f.openRemoteFile(
			consts.TokenFileSuffix,
			func(file storage.ImmutableFile) {
				f.tokenFile = file
				f.tokenReaderProvider = storage.NewReaderProvider(
					f.readLimiter, file.Name(),
					file, f.indexCache.TokenRegistry,
				)
			},
		)
		if err != nil {
			return err
		}
	}

	if f.offsetsFile == nil {
		err := f.openRemoteFile(
			consts.OffsetsFileSuffix,
			func(file storage.ImmutableFile) {
				f.offsetsFile = file
				f.offsetsReaderProvider = storage.NewReaderProvider(
					f.readLimiter, file.Name(),
					file, f.indexCache.OffsetsRegistry,
				)
			},
		)
		if err != nil {
			return err
		}
	}

	if f.idFile == nil {
		err := f.openRemoteFile(
			consts.IDFileSuffix,
			func(file storage.ImmutableFile) {
				f.idFile = file
				f.idReaderProvider = storage.NewReaderProvider(
					f.readLimiter, file.Name(),
					file, f.indexCache.IDRegistry,
				)
			},
		)
		if err != nil {
			return err
		}
	}

	if f.lidFile == nil {
		err := f.openRemoteFile(
			consts.LIDFileSuffix,
			func(file storage.ImmutableFile) {
				f.lidFile = file
				f.lidReaderProvider = storage.NewReaderProvider(
					f.readLimiter, file.Name(),
					file, f.indexCache.LIDRegistry,
				)
			},
		)
		if err != nil {
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

func (f *Remote) computeIndexSize() {
	if err := f.openIndex(); err != nil {
		logger.Error(
			"cannot open index file",
			zap.Error(err),
		)
		return
	}

	files := []storage.ImmutableFile{
		f.infoFile,
		f.tokenFile,
		f.offsetsFile,
		f.idFile,
		f.lidFile,
	}

	if f.IsSingleIndex() {
		files = []storage.ImmutableFile{
			f.legacyFile,
		}
	}

	f.info.IndexOnDisk = 0
	for _, file := range files {
		st, err := file.Stat()
		if err != nil {
			logger.Error(
				"can't stat index file",
				zap.String("file", file.Name()),
				zap.Error(err),
			)
			continue
		}

		f.info.IndexOnDisk += uint64(st.Size())
	}
}
