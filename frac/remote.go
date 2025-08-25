package frac

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
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

	info *Info

	useMu    sync.RWMutex
	suicided bool

	docsFile   storage.ImmutableFile
	docsCache  *cache.Cache[[]byte]
	docsReader storage.DocsReader

	indexFile   storage.ImmutableFile
	indexCache  *IndexCache
	indexReader storage.IndexReader

	loadMu   *sync.RWMutex
	isLoaded bool
	state    sealedState

	s3cli       *s3.Client
	readLimiter *storage.ReadLimiter
}

func NewRemote(
	ctx context.Context,
	baseFile string,
	readLimiter *storage.ReadLimiter,
	indexCache *IndexCache,
	docsCache *cache.Cache[[]byte],
	info *Info,
	config *Config,
	s3cli *s3.Client,
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

	if err := f.openIndex(); err != nil {
		logger.Error(
			"cannot open index file: any subsequent operation will fail",
			zap.String("fraction", filepath.Base(f.BaseFileName)),
			zap.Error(err),
		)
	}

	f.info = loadHeader(f.indexFile, f.indexReader)
	return f
}

func (f *Remote) Contains(mid seq.MID) bool {
	return f.info.IsIntersecting(mid, mid)
}

func (f *Remote) DataProvider(ctx context.Context) (DataProvider, func()) {
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

	if err := f.load(); err != nil {
		logger.Error(
			"will create empty data provider: cannot load remote fraction",
			zap.String("fraction", f.Info().Name()),
			zap.Error(err),
		)
		f.useMu.RUnlock()
		return EmptyDataProvider{}, func() {}
	}

	dp := f.createDataProvider(ctx)
	return dp, func() {
		dp.release()
		f.useMu.RUnlock()
	}
}

func (f *Remote) Info() *Info {
	return f.info
}

func (f *Remote) IsIntersecting(from, to seq.MID) bool {
	return f.info.IsIntersecting(from, to)
}

func (f *Remote) Offload(context.Context, storage.Uploader) (bool, error) {
	panic("BUG: remote fraction cannot be offloaded")
}

func (f *Remote) Suicide() {
	f.useMu.Lock()
	f.suicided = true
	f.useMu.Unlock()

	util.MustRemoveFileByPath(f.BaseFileName + consts.RemoteFractionSuffix)

	f.docsCache.Release()
	f.indexCache.Release()

	files := []string{
		filepath.Base(f.BaseFileName) + consts.DocsFileSuffix,
		filepath.Base(f.BaseFileName) + consts.SdocsFileSuffix,
		filepath.Base(f.BaseFileName) + consts.IndexFileSuffix,
	}

	err := f.s3cli.Remove(f.ctx, files...)
	if err != nil {
		logger.Info(
			"failed to delete files during suicide",
			zap.Any("files", files),
			zap.Error(err),
		)
	}
}

func (f *Remote) createDataProvider(ctx context.Context) *sealedDataProvider {
	return &sealedDataProvider{
		ctx:              ctx,
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

func (f *Remote) load() error {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if f.isLoaded {
		return nil
	}

	if err := f.openDocs(); err != nil {
		return err
	}

	if err := f.openIndex(); err != nil {
		return err
	}

	(&Loader{}).Load(&f.state, f.info, &f.indexReader)
	f.isLoaded = true

	return nil
}

func (f *Remote) openIndex() error {
	if f.indexFile != nil {
		return nil
	}

	name := filepath.Base(f.BaseFileName) + consts.IndexFileSuffix

	ok, err := f.s3cli.Exists(f.ctx, name)
	if err != nil {
		return fmt.Errorf(
			"cannot check existence of %q file: %w",
			consts.IndexFileSuffix, err,
		)
	}

	if ok {
		f.indexFile = s3.NewReader(f.ctx, f.s3cli, name)
		f.indexReader = storage.NewIndexReader(f.readLimiter, f.indexFile.Name(), f.indexFile, f.indexCache.Registry)
		return nil
	}

	return fmt.Errorf("missing %q file", consts.IndexFileSuffix)
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
