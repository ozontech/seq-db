package frac

import (
	"context"
	"errors"
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
	docsCache  *cache.ConcurrentCache[[]byte]
	docsReader storage.DocsReader

	// Per-section index files (new split format only).
	tokenFile   storage.ImmutableFile
	offsetsFile storage.ImmutableFile
	idFile      storage.ImmutableFile
	lidFile     storage.ImmutableFile

	legacyFile storage.ImmutableFile

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
	docsCache *cache.ConcurrentCache[[]byte],
	info *common.Info,
	config *Config,
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
		Config:       config,

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

	if err := f.loadInfo(); err != nil {
		// FIXME(dkharms): For now almost any availability issues with S3 will cause seq-db to panic
		// during initialisation phase. I wrote a small proposal on how we can reduce impact of such
		// events. https://github.com/ozontech/seq-db/issues/92
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

func (f *Remote) createDataProvider(ctx context.Context) (*sealedDataProvider, error) {
	if err := f.init(); err != nil {
		logger.Error(
			"will create empty data provider: cannot load remote fraction",
			zap.String("fraction", f.Info().Name()),
			zap.Error(err),
		)
		return nil, err
	}

	ir := f.indexReaders()
	return &sealedDataProvider{
		ctx:               ctx,
		fractionTypeLabel: "remote",

		info:          f.info,
		config:        f.Config,
		docsReader:    &f.docsReader,
		blocksOffsets: f.blocksData.BlocksOffsets,

		lidsTable:  f.blocksData.LIDsTable,
		lidsLoader: lids.NewLoader(f.info.BinaryDataVer, &ir.LID, cache.NewSession(f.indexCache.LIDs)),

		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.Info().BinaryDataVer, &ir.Token, cache.NewSession(f.indexCache.Tokens)),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.Info().BinaryDataVer, f.IsSingleIndex(), &ir.Token, cache.NewSession(f.indexCache.TokenTable)),

		idsTable: &f.blocksData.IDsTable,
		idsProvider: seqids.NewProvider(
			&ir.ID,
			cache.NewSession(f.indexCache.MIDs),
			cache.NewSession(f.indexCache.RIDs),
			cache.NewSession(f.indexCache.Params),
			&f.blocksData.IDsTable,
			f.info.BinaryDataVer,
		),
		skipMaskProvider: f.skipMaskProvider,
	}, nil
}

func (f *Remote) indexReaders() IndexReaders {
	if f.IsSingleIndex() {
		r := storage.NewIndexReader(
			f.readLimiter, f.legacyFile.Name(), f.legacyFile,
			cache.NewSession(f.indexCache.LegacyRegistry),
		)
		return IndexReaders{Token: r, Offsets: r, ID: r, LID: r}
	}

	return IndexReaders{
		Token: storage.NewIndexReader(
			f.readLimiter, f.tokenFile.Name(), f.tokenFile,
			cache.NewSession(f.indexCache.TokenRegistry),
		),

		Offsets: storage.NewIndexReader(
			f.readLimiter, f.offsetsFile.Name(), f.offsetsFile,
			cache.NewSession(f.indexCache.OffsetsRegistry),
		),

		ID: storage.NewIndexReader(
			f.readLimiter, f.idFile.Name(), f.idFile,
			cache.NewSession(f.indexCache.IDRegistry),
		),

		LID: storage.NewIndexReader(
			f.readLimiter, f.lidFile.Name(), f.lidFile,
			cache.NewSession(f.indexCache.LIDRegistry),
		),
	}
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

func (f *Remote) IsSingleIndex() bool {
	return f.info.BinaryDataVer < config.BinaryDataV3
}

// loadInfo loads the remote fraction information from available sources in priority order:
//  1. Local non-empty *.remote file (offload stores info inside .remote itself).
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

// tryLoadInfoLocal attempts to load fraction information from a local non-empty
// .remote file. This is the most preferred and modern approach, where all data
// is already present on disk. An empty .remote is a legacy marker and means the
// fraction was offloaded before info was stored inside .remote.
func (f *Remote) tryLoadInfoLocal() (err error) {
	var (
		file *os.File
		stat os.FileInfo
	)

	if file, err = os.Open(f.BaseFileName + consts.RemoteFractionSuffix); err != nil {
		return err
	}

	defer file.Close()

	if stat, err = file.Stat(); err != nil {
		return err
	}

	if stat.Size() == 0 {
		return errors.New("it's a legacy empty *.remote file")
	}

	f.info, err = loadInfo(file)
	return err
}

// tryLoadInfoRemote attempts to load fraction information from a remote .info file
// located on S3. This is an intermediate fallback: it is used when the local
// .remote is empty, but an .info file still exists on S3 (maintained for
// backward compatibility).
func (f *Remote) tryLoadInfoRemote() error {
	infoFile, err := f.openRemoteFile(consts.InfoFileSuffix, true)
	if err == nil {
		f.info, err = loadInfo(infoFile)
	}
	return err
}

// loadInfoLegacy loads fraction information from the legacy index stored on S3.
// This is the oldest fallback, used when only an empty *.remote file exists locally
// and a single *.index file resides on S3 containing all necessary data.
func (f *Remote) loadInfoLegacy() (err error) {
	if err = f.openIndexLegacyRemote(); err != nil {
		return err
	}

	reader := storage.NewIndexReader(
		f.readLimiter, f.legacyFile.Name(), f.legacyFile,
		cache.NewSession(f.indexCache.LegacyRegistry),
	)

	f.info, err = loadInfoLegacy(reader)
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
		(&LegacyLoader{}).Load(
			&f.blocksData, f.info,
			storage.NewIndexReader(
				f.readLimiter, f.legacyFile.Name(), f.legacyFile,
				cache.NewSession(f.indexCache.LegacyRegistry),
			),
		)

		f.isInited = true
		return nil
	}

	(&Loader{}).Load(&f.blocksData, f.info, f.indexReaders())

	f.isInited = true
	return nil
}

func (f *Remote) openIndexLegacyRemote() (err error) {
	if f.legacyFile == nil {
		f.legacyFile, err = f.openRemoteFile(consts.IndexFileSuffix, true)
	}
	return err
}

func (f *Remote) openIndex() error {
	if f.IsSingleIndex() {
		return f.openIndexLegacyRemote()
	}

	var err error

	if f.tokenFile == nil {
		if f.tokenFile, err = f.openRemoteFile(consts.TokenFileSuffix, true); err != nil {
			return err
		}
	}

	if f.offsetsFile == nil {
		if f.offsetsFile, err = f.openRemoteFile(consts.OffsetsFileSuffix, true); err != nil {
			return err
		}
	}

	if f.idFile == nil {
		if f.idFile, err = f.openRemoteFile(consts.IDFileSuffix, true); err != nil {
			return err
		}
	}

	if f.lidFile == nil {
		if f.lidFile, err = f.openRemoteFile(consts.LIDFileSuffix, true); err != nil {
			return err
		}
	}

	return nil
}

// openRemoteFile returns (nil, nil) if the file is missing and mustExist is false.
func (f *Remote) openRemoteFile(suffix string, mustExist bool) (storage.ImmutableFile, error) {
	name := filepath.Base(f.BaseFileName) + suffix
	ok, err := f.s3cli.Exists(f.ctx, name)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot check existence of %q file: %w",
			suffix, err,
		)
	}

	if !ok {
		if mustExist {
			return nil, fmt.Errorf("missing %q file", suffix)
		}
		return nil, nil
	}

	return s3.NewReader(f.ctx, f.s3cli, name), nil
}

func (f *Remote) openDocs() error {
	if f.docsFile != nil {
		return nil
	}

	docsFile, err := f.openRemoteFile(consts.DocsFileSuffix, false)
	if err != nil {
		return err
	}

	if docsFile == nil {
		docsFile, err = f.openRemoteFile(consts.SdocsFileSuffix, false)
		if err != nil {
			return err
		}
		if docsFile == nil {
			return fmt.Errorf("missing %q and %q files", consts.DocsFileSuffix, consts.SdocsFileSuffix)
		}
	}

	f.docsFile = docsFile
	f.docsReader = storage.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
	return nil
}

func (f *Remote) computeIndexSize() {
	if err := f.openIndex(); err != nil {
		logger.Error(
			"cannot open index file",
			zap.Error(err),
		)
		return
	}

	f.info.IndexOnDisk = f.info.InfoOnDisk
	files := []storage.ImmutableFile{
		f.tokenFile,
		f.offsetsFile,
		f.idFile,
		f.lidFile,
	}

	if f.IsSingleIndex() {
		f.info.IndexOnDisk = 0
		files = []storage.ImmutableFile{
			f.legacyFile,
		}
	}

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
