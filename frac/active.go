package frac

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type Active struct {
	Config *Config

	BaseFileName string

	useMu    sync.RWMutex
	suicided bool
	released bool

	infoMu sync.RWMutex
	info   *Info

	MIDs *UInt64s
	RIDs *UInt64s

	DocBlocks *UInt64s

	TokenList *TokenList

	DocsPositions *DocsPositions

	docsFile   *os.File
	docsReader disk.DocsReader
	sortReader disk.DocsReader
	docsCache  *cache.Cache[[]byte]
	sortCache  *cache.Cache[[]byte]

	metaFile   *os.File
	metaReader disk.DocBlocksReader

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
	readLimiter *disk.ReadLimiter,
	docsCache *cache.Cache[[]byte],
	sortCache *cache.Cache[[]byte],
	config *Config,
) *Active {
	docsFile, docsStats := mustOpenFile(baseFileName+consts.DocsFileSuffix, conf.SkipFsync)
	metaFile, metaStats := mustOpenFile(baseFileName+consts.MetaFileSuffix, conf.SkipFsync)

	f := &Active{
		TokenList:     NewActiveTokenList(conf.IndexWorkers),
		DocsPositions: NewSyncDocsPositions(),
		MIDs:          NewIDs(),
		RIDs:          NewIDs(),
		DocBlocks:     NewIDs(),

		docsFile:   docsFile,
		docsCache:  docsCache,
		sortCache:  sortCache,
		docsReader: disk.NewDocsReader(readLimiter, docsFile, docsCache),
		sortReader: disk.NewDocsReader(readLimiter, docsFile, sortCache),

		metaFile:   metaFile,
		metaReader: disk.NewDocBlocksReader(readLimiter, metaFile),

		indexer: activeIndexer,
		writer:  NewActiveWriter(docsFile, metaFile, docsStats.Size(), metaStats.Size(), conf.SkipFsync),

		BaseFileName: baseFileName,
		info:         NewInfo(baseFileName, uint64(docsStats.Size()), uint64(metaStats.Size())),
		Config:       config,
	}

	// use of 0 as keys in maps is prohibited – it's system key, so add first element
	f.MIDs.Append(systemMID)
	f.RIDs.Append(systemRID)

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	return f
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
	logger.Info("start replaying...")

	targetSize := f.info.MetaOnDisk
	t := time.Now()

	docsPos := uint64(0)
	metaPos := uint64(0)
	step := targetSize / 10
	next := step

	sw := stopwatch.New()
	wg := sync.WaitGroup{}

out:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			meta, metaSize, err := f.metaReader.ReadDocBlock(int64(metaPos))
			if err == io.EOF {
				if metaSize != 0 {
					logger.Warn("last meta block is partially written, skipping it")
				}
				break out
			}
			if err != nil && err != io.EOF {
				return err
			}

			if metaPos > next {
				next += step
				progress := float64(metaPos) / float64(targetSize) * 100
				logger.Info("replaying batch, meta",
					zap.Uint64("from", metaPos),
					zap.Uint64("to", metaPos+metaSize),
					zap.Uint64("target", targetSize),
					util.ZapFloat64WithPrec("progress_percentage", progress, 2),
				)
			}

			docBlockLen := disk.DocBlock(meta).GetExt1()
			disk.DocBlock(meta).SetExt2(docsPos) // todo: remove this on next release

			docsPos += docBlockLen
			metaPos += metaSize

			wg.Add(1)
			f.indexer.Index(f, meta, &wg, sw)
		}
	}

	wg.Wait()

	tookSeconds := util.DurationToUnit(time.Since(t), "s")
	throughputRaw := util.SizeToUnit(f.info.DocsRaw, "mb") / tookSeconds
	throughputMeta := util.SizeToUnit(f.info.MetaOnDisk, "mb") / tookSeconds
	logger.Info("active fraction replayed",
		zap.String("name", f.info.Name()),
		zap.Uint32("docs_total", f.info.DocsTotal),
		util.ZapUint64AsSizeStr("docs_size", docsPos),
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
}, []string{"stage"})

// Append causes data to be written on disk and sends metas to index workers
func (f *Active) Append(docs, metas []byte, wg *sync.WaitGroup) (err error) {
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

func (f *Active) DataProvider(ctx context.Context) (DataProvider, func()) {
	f.useMu.RLock()

	if f.suicided || f.released || f.Info().DocsTotal == 0 { // it is empty active fraction state
		if f.suicided {
			metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
		}
		f.useMu.RUnlock()
		return EmptyDataProvider{}, func() {}
	}

	// it is ordinary active fraction state
	dp := f.createDataProvider(ctx)
	return dp, func() {
		dp.release()
		f.useMu.RUnlock()
	}
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

func (f *Active) Info() *Info {
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
	f.useMu.Lock()
	f.released = true
	f.useMu.Unlock()

	f.releaseMem()

	if !f.Config.KeepMetaFile {
		f.removeMetaFile()
	}

	if !f.Config.SkipSortDocs {
		// we use sorted docs in sealed fraction so we can remove original docs of active fraction
		f.removeDocsFiles()
	}
}

func (f *Active) Suicide() {
	f.useMu.Lock()
	released := f.released
	f.suicided = true
	f.released = true
	f.useMu.Unlock()

	if released { // fraction can be suicided after release
		if f.Config.KeepMetaFile {
			f.removeMetaFile() // meta was not removed while release
		}
		if f.Config.SkipSortDocs {
			f.removeDocsFiles() // docs was not removed while release
		}
	} else { // was not release
		f.releaseMem()
		f.removeMetaFile()
		f.removeDocsFiles()
	}
}

func (f *Active) releaseMem() {
	f.writer.Stop()
	f.TokenList.Stop()

	f.docsCache.Release()
	f.sortCache.Release()

	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}

	f.RIDs = nil
	f.MIDs = nil
	f.TokenList = nil
	f.DocsPositions = nil
}

func (f *Active) removeDocsFiles() {
	if err := f.docsFile.Close(); err != nil {
		logger.Error("can't close docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
	if err := os.Remove(f.docsFile.Name()); err != nil {
		logger.Error("can't delete docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
}

func (f *Active) removeMetaFile() {
	if err := os.Remove(f.metaFile.Name()); err != nil {
		logger.Error("can't delete metas file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
}
