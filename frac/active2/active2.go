package active2

import (
	"context"
	"io"
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
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type Active2 struct {
	Config *frac.Config

	BaseFileName string

	useMu    sync.RWMutex
	suicided bool
	released bool

	indexMu sync.RWMutex
	info    *frac.Info
	indexes Indexes
	indexer Indexer

	docsFile   *os.File
	docsReader disk.DocsReader
	sortReader disk.DocsReader
	docsCache  *cache.Cache[[]byte]
	sortCache  *cache.Cache[[]byte]

	metaFile   *os.File
	metaReader disk.DocBlocksReader

	writer *frac.ActiveWriter
}

func New(
	baseFileName string,
	readLimiter *disk.ReadLimiter,
	docsCache *cache.Cache[[]byte],
	sortCache *cache.Cache[[]byte],
	config *frac.Config,
) *Active2 {
	docsFile, docsStats := mustOpenFile(baseFileName+consts.DocsFileSuffix, conf.SkipFsync)
	metaFile, metaStats := mustOpenFile(baseFileName+consts.MetaFileSuffix, conf.SkipFsync)

	f := &Active2{
		docsFile:   docsFile,
		docsCache:  docsCache,
		sortCache:  sortCache,
		docsReader: disk.NewDocsReader(readLimiter, docsFile, docsCache),
		sortReader: disk.NewDocsReader(readLimiter, docsFile, sortCache),

		metaFile:   metaFile,
		metaReader: disk.NewDocBlocksReader(readLimiter, metaFile),

		writer: frac.NewActiveWriter(docsFile, metaFile, docsStats.Size(), metaStats.Size(), conf.SkipFsync),

		BaseFileName: baseFileName,
		info:         frac.NewInfo(baseFileName, uint64(docsStats.Size()), uint64(metaStats.Size())),
		Config:       config,
	}

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

func (f *Active2) Replay(ctx context.Context) error {
	logger.Info("start replaying...")

	targetSize := f.info.MetaOnDisk
	t := time.Now()

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

			metaPos += metaSize

			wg.Add(1)
			f.indexer.Index(meta, sw, func(index *memIndex, err error) {
				if err != nil {
					logger.Fatal("bulk indexing error", zap.Error(err))
				}
				f.addIndex(index)
				wg.Done()
			})
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
}, []string{"stage"})

// Append causes data to be written on disk and sends metas to index workers
func (f *Active2) Append(docs, meta []byte, wg *sync.WaitGroup) (err error) {
	sw := stopwatch.New()
	m := sw.Start("append")
	if err = f.writer.Write(docs, meta, sw); err != nil {
		m.Stop()
		return err
	}
	f.updateDiskStats(uint64(len(docs)), uint64(len(meta)))

	f.indexer.Index(meta, sw, func(index *memIndex, err error) {
		if err != nil {
			logger.Fatal("bulk indexing error", zap.Error(err))
		}
		f.addIndex(index)
		wg.Done()
	})

	m.Stop()
	sw.Export(bulkStagesSeconds)
	return nil
}

func (f *Active2) updateDiskStats(docsLen, metaLen uint64) {
	f.indexMu.Lock()
	f.info.DocsOnDisk += docsLen
	f.info.MetaOnDisk += metaLen
	f.indexMu.Unlock()
}

func (f *Active2) addIndex(index *memIndex) {
	maxMID := index.ids[0].MID
	minMID := index.ids[len(index.ids)-1].MID

	f.indexMu.Lock()
	defer f.indexMu.Unlock()

	f.indexes.Add(index)

	if f.info.From > minMID {
		f.info.From = minMID
	}
	if f.info.To < maxMID {
		f.info.To = maxMID
	}
	f.info.DocsRaw += index.docsSize
	f.info.DocsTotal += index.docsCount
}

func (f *Active2) String() string {
	return frac.FracToString(f, "active2")
}

func (f *Active2) DataProvider(ctx context.Context) (frac.DataProvider, func()) {
	f.useMu.RLock()

	if f.suicided {
		f.useMu.RUnlock()
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
		return frac.EmptyDataProvider{}, func() {}
	}

	if f.released {
		f.useMu.RUnlock()
		metric.CountersTotal.WithLabelValues("fraction_released").Inc()
		return frac.EmptyDataProvider{}, func() {}
	}

	dp := f.createDataProvider(ctx)

	if dp.info.DocsTotal == 0 { // it is empty active fraction state
		f.useMu.RUnlock()
		metric.CountersTotal.WithLabelValues("fraction_empty").Inc()
		return frac.EmptyDataProvider{}, func() {}
	}

	// it is ordinary active fraction state
	return dp, f.useMu.RUnlock
}

func (f *Active2) createDataProvider(ctx context.Context) *dataProvider {
	f.indexMu.RLock()
	info := *f.info // copy
	indexes := f.indexes.Indexes()
	f.indexMu.RUnlock()

	return &dataProvider{
		ctx:        ctx,
		config:     f.Config,
		info:       &info,
		indexes:    indexes,
		docsReader: &f.docsReader,
	}
}

func (f *Active2) Info() *frac.Info {
	f.indexMu.RLock()
	defer f.indexMu.RUnlock()

	cp := *f.info // copy
	return &cp
}

func (f *Active2) Contains(id seq.MID) bool {
	return f.Info().IsIntersecting(id, id)
}

func (f *Active2) IsIntersecting(from, to seq.MID) bool {
	return f.Info().IsIntersecting(from, to)
}

func (f *Active2) Release() {
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

func (f *Active2) Suicide() {
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

func (f *Active2) releaseMem() {
	f.writer.Stop()
	f.indexes.StopMergeLoop()

	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
}

func (f *Active2) removeDocsFiles() {
	if err := f.docsFile.Close(); err != nil {
		logger.Error("can't close docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
	if err := os.Remove(f.docsFile.Name()); err != nil {
		logger.Error("can't delete docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
}

func (f *Active2) removeMetaFile() {
	if err := os.Remove(f.metaFile.Name()); err != nil {
		logger.Error("can't delete metas file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
}
