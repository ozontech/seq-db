package frac

import (
	"context"
	"path"
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
	state    *State

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

		state:  &State{},
		loadMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		info:         info,
		BaseFileName: baseFile,
		Config:       config,

		s3cli: s3cli,
	}

	// fast path if fraction-info cache exists AND it has valid index size
	if info != nil && info.IndexOnDisk > 0 {
		return f
	}

	f.openIndex()
	f.info = loadHeader(f.BaseFileName, f.indexFile, f.indexReader)
	f.info.StorageType = storage.TypeRemote

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

	f.load()
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
		path.Base(f.BaseFileName) + consts.DocsFileSuffix,
		path.Base(f.BaseFileName) + consts.SdocsFileSuffix,
		path.Base(f.BaseFileName) + consts.IndexFileSuffix,
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

func (f *Remote) load() {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {
		f.openDocs()
		f.openIndex()

		(&Loader{}).Load(f.state, f.info, &f.indexReader)
		f.isLoaded = true
	}
}

func (f *Remote) openIndex() {
	if f.indexFile == nil {
		name := path.Base(f.BaseFileName) + consts.IndexFileSuffix
		f.indexFile = s3.NewReader(f.ctx, f.s3cli, name)
		f.indexReader = storage.NewIndexReader(f.readLimiter, f.indexFile.Name(), f.indexFile, f.indexCache.Registry)
	}
}

func (f *Remote) openDocs() {
	if f.docsFile == nil {
		pickedName := path.Base(f.BaseFileName) + consts.DocsFileSuffix
		sortedName := path.Base(f.BaseFileName) + consts.SdocsFileSuffix

		if ok, _ := f.s3cli.Exists(f.ctx, pickedName); !ok {
			pickedName = sortedName
		}

		f.docsFile = s3.NewReader(f.ctx, f.s3cli, pickedName)
		f.docsReader = storage.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
	}
}
