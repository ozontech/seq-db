package frac

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

var (
	_ Fraction = (*Active)(nil)
)

type Active struct {
	Config *Config

	BaseFileName string

	infoMu sync.RWMutex
	info   *common.Info

	MIDs *UInt64s
	RIDs *UInt64s

	DocBlocks *UInt64s

	TokenList *TokenList

	DocsPositions *DocsPositions

	docsFile   *os.File
	docsReader storage.DocsReader
	sortReader storage.DocsReader
	docsCache  *cache.Cache[[]byte]
	sortCache  *cache.Cache[[]byte]

	metaFile   *os.File
	metaReader *storage.DocBlocksReader
	walReader  *storage.WalReader

	writer  *ActiveWriter
	indexer *ActiveIndexer
}

const (
	systemMID = math.MaxUint64
	systemRID = math.MaxUint64
)

var systemSeqID = seq.ID{
	MID: systemMID,
	RID: systemRID,
}

func NewActive(
	baseFileName string,
	activeIndexer *ActiveIndexer,
	readLimiter *storage.ReadLimiter,
	docsCache *cache.Cache[[]byte],
	sortCache *cache.Cache[[]byte],
	cfg *Config,
) *Active {
	docsFile, docsStats := mustOpenFile(baseFileName+consts.DocsFileSuffix, config.SkipFsync)

	metaFile, writer, metaReader, walReader, metaSize := mustOpenMetaWriter(baseFileName, readLimiter, docsFile, docsStats)

	f := &Active{
		TokenList:     NewActiveTokenList(config.IndexWorkers),
		DocsPositions: NewSyncDocsPositions(),
		MIDs:          NewIDs(),
		RIDs:          NewIDs(),
		DocBlocks:     NewIDs(),

		docsFile:   docsFile,
		docsCache:  docsCache,
		sortCache:  sortCache,
		docsReader: storage.NewDocsReader(readLimiter, docsFile, docsCache),
		sortReader: storage.NewDocsReader(readLimiter, docsFile, sortCache),

		metaFile:   metaFile,
		metaReader: metaReader,
		walReader:  walReader,

		indexer: activeIndexer,
		writer:  writer,

		BaseFileName: baseFileName,
		info:         common.NewInfo(baseFileName, uint64(docsStats.Size()), metaSize),
		Config:       cfg,
	}

	// use of 0 as keys in maps is prohibited – it's system key, so add first element
	f.MIDs.Append(systemMID)
	f.RIDs.Append(systemRID)

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	return f
}

func mustOpenMetaWriter(
	baseFileName string,
	readLimiter *storage.ReadLimiter,
	docsFile *os.File,
	docsStats os.FileInfo) (*os.File, *ActiveWriter, *storage.DocBlocksReader, *storage.WalReader, uint64) {
	legacyMetaFileName := baseFileName + consts.MetaFileSuffix

	if _, err := os.Stat(legacyMetaFileName); err == nil {
		// .meta file exists
		metaFile, metaStats := mustOpenFile(legacyMetaFileName, config.SkipFsync)
		metaSize := uint64(metaStats.Size())
		metaReader := storage.NewDocBlocksReader(readLimiter, metaFile)
		writer := NewActiveWriterLegacy(docsFile, metaFile, docsStats.Size(), metaStats.Size(), config.SkipFsync)
		logger.Info("using legacy meta file format", zap.String("fraction", baseFileName))
		return metaFile, writer, &metaReader, nil, metaSize
	}

	logger.Info("using new WAL format", zap.String("fraction", baseFileName))
	walFileName := baseFileName + consts.WalFileSuffix
	metaFile, metaStats := mustOpenFile(walFileName, config.SkipFsync)
	metaSize := uint64(metaStats.Size())
	writer := NewActiveWriter(docsFile, metaFile, docsStats.Size(), metaStats.Size(), config.SkipFsync)
	walReader, err := storage.NewWalReader(readLimiter, metaFile, baseFileName)
	if err != nil {
		logger.Fatal("failed to initialize WAL reader", zap.String("fraction", baseFileName), zap.Error(err))
	}
	return metaFile, writer, nil, walReader, metaSize
}

func mustOpenFile(name string, skipFsync bool) (*os.File, os.FileInfo) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't create docs file", zap.String("file", name), zap.Error(err))
	}

	if !skipFsync {
		parentDirPath := filepath.Dir(name)
		util.MustSyncPath(parentDirPath)
	}

	stat, err := file.Stat()
	if err != nil {
		logger.Fatal("can't stat docs file", zap.String("file", name), zap.Error(err))
	}
	return file, stat
}

func (f *Active) Replay(ctx context.Context) error {
	if f.metaReader != nil {
		return f.replayMetaFileLegacy(ctx)
	}
	return f.replayWalFile(ctx)
}

func (f *Active) replayWalFile(ctx context.Context) error {
	logger.Info("start replaying WAL file...", zap.String("name", f.info.Name()))

	t := time.Now()

	step := f.info.MetaOnDisk / 10
	next := step

	sw := stopwatch.New()
	wg := sync.WaitGroup{}
	var corruptions uint64

	for entry := range f.walReader.Entries() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if entry.Err != nil {
			return entry.Err
		}
		corruptions = entry.Corruptions

		if uint64(entry.Offset) > next {
			next += step
			progress := float64(uint64(entry.Offset)) / float64(f.info.MetaOnDisk) * 100
			logger.Info("replaying batch, meta",
				zap.String("name", f.info.Name()),
				zap.Int64("from", entry.Offset),
				zap.Int64("to", entry.Offset+entry.Size),
				zap.Uint64("target", f.info.MetaOnDisk),
				util.ZapFloat64WithPrec("progress_percentage", progress, 2),
			)
		}

		wg.Add(1)
		f.indexer.Index(f, entry.Data, &wg, sw)
	}

	wg.Wait()
	if corruptions > 0 {
		metric.WALCorruptionsTotal.Add(float64(corruptions))
		if err := f.backupCorruptedFiles(); err != nil {
			logger.Error("failed to copy a corrupted WAL file",
				zap.String("name", f.info.Name()),
				zap.Error(err),
			)
		}
	}

	tookSeconds := util.DurationToUnit(time.Since(t), "s")
	throughputRaw := util.SizeToUnit(f.info.DocsRaw, "mb") / tookSeconds
	throughputMeta := util.SizeToUnit(f.info.MetaOnDisk, "mb") / tookSeconds
	logger.Info("active fraction replayed",
		zap.String("name", f.info.Name()),
		zap.Uint32("docs_total", f.info.DocsTotal),
		util.ZapUint64AsSizeStr("docs_size", f.info.DocsOnDisk),
		util.ZapFloat64WithPrec("took_s", tookSeconds, 1),
		util.ZapFloat64WithPrec("throughput_raw_mb_sec", throughputRaw, 1),
		util.ZapFloat64WithPrec("throughput_meta_mb_sec", throughputMeta, 1),
	)
	return nil
}

// backupCorruptedFiles saves wal and docs file in a directory with corrupted files for later analysis
func (f *Active) backupCorruptedFiles() error {
	brokenDir := filepath.Join(filepath.Dir(f.BaseFileName), consts.BrokenDir)
	if err := os.MkdirAll(brokenDir, 0o777); err != nil {
		return fmt.Errorf("create dir %s, err: %w", brokenDir, err)
	}

	fracName := filepath.Base(f.BaseFileName)

	walSrc := f.BaseFileName + consts.WalFileSuffix
	walDst := filepath.Join(brokenDir, fracName+consts.WalFileSuffix)
	if err := util.CopyFile(walSrc, walDst); err != nil {
		return fmt.Errorf("copy from %s to %s, err: %w", walSrc, walDst, err)
	}

	docSrc := f.BaseFileName + consts.DocsFileSuffix
	docDst := filepath.Join(brokenDir, fracName+consts.DocsFileSuffix)
	if err := util.CopyFile(docSrc, docDst); err != nil {
		return fmt.Errorf("copy from %s to %s, err: %w", docSrc, docDst, err)
	}

	return nil
}

// replayMetaFileLegacy replays legacy *.meta files. Only basic corruption detection support is implemented
func (f *Active) replayMetaFileLegacy(ctx context.Context) error {
	logger.Info("start replaying...", zap.String("name", f.info.Name()))

	t := time.Now()

	offset := uint64(0)
	step := f.info.MetaOnDisk / 10
	next := step

	sw := stopwatch.New()
	wg := sync.WaitGroup{}

out:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			meta, metaSize, err := f.metaReader.ReadDocBlock(int64(offset))
			if err == io.EOF {
				if metaSize != 0 {
					logger.Warn("last meta block is partially written, skipping it")
				}
				break out
			}
			if err != nil && err != io.EOF {
				return err
			}

			if offset > next {
				next += step
				progress := float64(offset) / float64(f.info.MetaOnDisk) * 100
				logger.Info("replaying batch, meta",
					zap.String("name", f.info.Name()),
					zap.Uint64("from", offset),
					zap.Uint64("to", offset+metaSize),
					zap.Uint64("target", f.info.MetaOnDisk),
					util.ZapFloat64WithPrec("progress_percentage", progress, 2),
				)
			}
			offset += metaSize

			wg.Add(1)

			walBlock := storage.PackDocBlockToWalBlock(meta)
			f.indexer.Index(f, walBlock, &wg, sw)
		}
	}

	wg.Wait()

	tookSeconds := util.DurationToUnit(time.Since(t), "s")
	throughputRaw := util.SizeToUnit(f.info.DocsRaw, "mb") / tookSeconds
	throughputMeta := util.SizeToUnit(f.info.MetaOnDisk, "mb") / tookSeconds
	logger.Info("active fraction replayed",
		zap.String("name", f.info.Name()),
		zap.Uint32("docs_total", f.info.DocsTotal),
		util.ZapUint64AsSizeStr("docs_size", f.info.DocsOnDisk),
		util.ZapFloat64WithPrec("took_s", tookSeconds, 1),
		util.ZapFloat64WithPrec("throughput_raw_mb_sec", throughputRaw, 1),
		util.ZapFloat64WithPrec("throughput_meta_mb_sec", throughputMeta, 1),
	)
	return nil
}

var bulkStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "seq_db_store",
	Subsystem: "bulk",
	Name:      "stages_seconds",
	Buckets:   metric.SecondsBuckets,
	Help:      "Bulk processing time by stage",
}, []string{"stage"})

// Append causes data to be written on disk and sends metas to index workers
func (f *Active) Append(docs storage.DocBlock, metas storage.WalBlock, wg *sync.WaitGroup) (err error) {
	sw := stopwatch.New()
	m := sw.Start("append")
	if err = f.writer.Write(docs, metas, sw); err != nil {
		m.Stop()
		return err
	}
	f.updateDiskStats(uint64(len(docs)), uint64(len(metas)))
	f.indexer.Index(f, metas, wg, sw)
	m.Stop()
	sw.Export(bulkStagesSeconds)
	return nil
}

func (f *Active) GetAllDocuments() []uint32 {
	return f.TokenList.GetAllTokenLIDs().GetLIDs(f.MIDs, f.RIDs)
}

func (f *Active) updateDiskStats(docsLen, metaLen uint64) {
	f.infoMu.Lock()
	f.info.DocsOnDisk += docsLen
	f.info.MetaOnDisk += metaLen
	f.infoMu.Unlock()
}

func (f *Active) AppendIDs(ids []seq.ID) []uint32 {
	// take both locks, append in both arrays at once
	// i.e. so one thread wouldn't append between other thread appends

	lidsList := make([]uint32, 0, len(ids))
	f.MIDs.mu.Lock()
	f.RIDs.mu.Lock()
	defer f.RIDs.mu.Unlock()
	defer f.MIDs.mu.Unlock()

	for _, id := range ids {
		lidsList = append(lidsList, f.MIDs.append(uint64(id.MID)))
		f.RIDs.append(uint64(id.RID))
	}

	return lidsList
}

func (f *Active) UpdateStats(minMID, maxMID seq.MID, docCount uint32, sizeCount uint64) {
	f.infoMu.Lock()
	defer f.infoMu.Unlock()

	if f.info.From > minMID {
		f.info.From = minMID
	}
	if f.info.To < maxMID {
		f.info.To = maxMID
	}
	f.info.DocsTotal += docCount
	f.info.DocsRaw += sizeCount
}

func (f *Active) String() string {
	return fracToString(f, "active")
}

func (f *Active) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	if f.Info().DocsTotal == 0 { // it is empty active fraction state
		return nil, nil
	}

	dp := f.createDataProvider(ctx)
	defer dp.release()

	return dp.Fetch(ids)
}

func (f *Active) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	if f.Info().DocsTotal == 0 { // it is empty active fraction state
		metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
		return &seq.QPR{Aggs: make([]seq.AggregatableSamples, len(params.AggQ))}, nil
	}

	dp := f.createDataProvider(ctx)
	defer dp.release()

	return dp.Search(params)
}

func (f *Active) createDataProvider(ctx context.Context) *activeDataProvider {
	return &activeDataProvider{
		ctx:    ctx,
		config: f.Config,
		info:   f.Info(),

		mids:      f.MIDs,
		rids:      f.RIDs,
		tokenList: f.TokenList,

		blocksOffsets: f.DocBlocks.GetVals(),
		docsPositions: f.DocsPositions,
		docsReader:    &f.docsReader,
	}
}

func (f *Active) Info() *common.Info {
	f.infoMu.RLock()
	defer f.infoMu.RUnlock()

	cp := *f.info // copy
	return &cp
}

func (f *Active) Contains(id seq.MID) bool {
	return f.Info().IsIntersecting(id, id)
}

func (f *Active) IsIntersecting(from, to seq.MID) bool {
	return f.Info().IsIntersecting(from, to)
}

func (f *Active) Release() {
	f.releaseMem()

	if !f.Config.KeepMetaFile {
		util.RemoveFile(f.metaFile.Name())
	}

	if !f.Config.SkipSortDocs {
		// we use sorted docs in sealed fraction so we can remove original docs of active fraction
		util.RemoveFile(f.docsFile.Name())
	}
}

func (f *Active) Suicide() {
	f.releaseMem()

	util.RemoveFile(f.metaFile.Name())
	util.RemoveFile(f.docsFile.Name())
	util.RemoveFile(f.BaseFileName + consts.SdocsFileSuffix)
}

func (f *Active) releaseMem() {
	f.writer.Stop()
	f.TokenList.Stop()

	f.docsCache.Release()
	f.sortCache.Release()

	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
	if err := f.docsFile.Close(); err != nil {
		logger.Error("can't close docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}

	f.RIDs = nil
	f.MIDs = nil
	f.TokenList = nil
	f.DocsPositions = nil
}
