package active2

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/active"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type Active2 struct {
	Config *frac.Config

	BaseFileName string

	indexer *Indexer

	indexes *memIndexPool
	merger  *MergeManager

	docsFile   *os.File
	docsReader storage.DocsReader
	sortReader storage.DocsReader
	docsCache  *cache.Cache[[]byte]
	sortCache  *cache.Cache[[]byte]

	metaFile   *os.File
	metaReader storage.DocBlocksReader

	writer *active.Writer
}

const MergerWorkers = 2

func New(
	baseFileName string,
	cfg *frac.Config,
	indexer *Indexer,
	readLimiter *storage.ReadLimiter,
	docsCache *cache.Cache[[]byte],
	sortCache *cache.Cache[[]byte],
) *Active2 {
	docsFile, docsStats := util.MustOpenFile(baseFileName+consts.DocsFileSuffix, config.SkipFsync)
	metaFile, metaStats := util.MustOpenFile(baseFileName+consts.MetaFileSuffix, config.SkipFsync)

	info := frac.NewInfo(baseFileName, uint64(docsStats.Size()), uint64(metaStats.Size()))
	indexes := NewIndexPool(info)
	merger := NewMergeManager(indexes, MergerWorkers)

	f := &Active2{
		BaseFileName: baseFileName,
		Config:       cfg,
		indexer:      indexer,
		indexes:      indexes,
		merger:       merger,

		docsFile:   docsFile,
		docsCache:  docsCache,
		sortCache:  sortCache,
		docsReader: storage.NewDocsReader(readLimiter, docsFile, docsCache),
		sortReader: storage.NewDocsReader(readLimiter, docsFile, sortCache),

		metaFile:   metaFile,
		metaReader: storage.NewDocBlocksReader(readLimiter, metaFile),

		writer: active.NewWriter(docsFile, metaFile, docsStats.Size(), metaStats.Size(), config.SkipFsync),
	}

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	return f
}

func (f *Active2) Replay(ctx context.Context) error {

	info := f.indexes.info

	logger.Info("start replaying...", zap.String("name", info.Name()))

	t := time.Now()

	offset := uint64(0)
	step := info.MetaOnDisk / 10
	wg := sync.WaitGroup{}
	next := step

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
				progress := float64(offset) / float64(info.MetaOnDisk) * 100
				logger.Info("replaying batch, meta",
					zap.String("name", info.Name()),
					zap.Uint64("from", offset),
					zap.Uint64("to", offset+metaSize),
					zap.Uint64("target", info.MetaOnDisk),
					util.ZapFloat64WithPrec("progress_percentage", progress, 2),
				)
			}
			offset += metaSize

			wg.Add(1)
			f.indexer.Index(meta, func(idx *memIndex, err error) {
				if err != nil {
					logger.Fatal("bulk indexing error", zap.Error(err))
				}
				f.indexes.Add(idx, 0, 0)
				f.merger.triggerMerge()
				wg.Done()
			})
		}
	}

	wg.Wait()

	tookSeconds := util.DurationToUnit(time.Since(t), "s")
	throughputRaw := util.SizeToUnit(info.DocsRaw, "mb") / tookSeconds
	throughputMeta := util.SizeToUnit(info.MetaOnDisk, "mb") / tookSeconds
	logger.Info("active fraction replayed",
		zap.String("name", info.Name()),
		zap.Uint32("docs_total", info.DocsTotal),
		util.ZapUint64AsSizeStr("docs_size", info.DocsOnDisk),
		util.ZapFloat64WithPrec("took_s", tookSeconds, 1),
		util.ZapFloat64WithPrec("throughput_raw_mb_sec", throughputRaw, 1),
		util.ZapFloat64WithPrec("throughput_meta_mb_sec", throughputMeta, 1),
	)
	return nil
}

func (f *Active2) Append(docs, meta []byte, wg *sync.WaitGroup) (err error) {
	sw := stopwatch.New()
	ma := sw.Start("append")
	if err = f.writer.Write(docs, meta, sw); err != nil {
		ma.Stop()
		return err
	}

	mi := sw.Start("send_to_indexer")
	f.indexer.Index(meta, func(idx *memIndex, err error) {
		if err != nil {
			logger.Fatal("bulk indexing error", zap.Error(err))
		}
		f.indexes.Add(idx, uint64(len(docs)), uint64(len(meta)))
		f.merger.triggerMerge()
		wg.Done()
	})
	mi.Stop()

	ma.Stop()
	sw.Export(bulkStagesSeconds)
	return nil
}

func (f *Active2) String() string {
	return frac.FracToString(f, "active")
}

func (f *Active2) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	defer sw.Export(fetcherStagesSec)

	t := sw.Start("total")

	ss, release := f.indexes.Snapshot()
	defer release()

	if ss.info.DocsTotal == 0 { // it is empty active fraction state
		return nil, nil
	}

	res := make([][]byte, len(ids))
	for _, index := range ss.indexes {
		fetchIndex := fetchIndex{index: index, docsReader: &f.docsReader}
		if err := processor.IndexFetch(ids, sw, &fetchIndex, res); err != nil {
			return nil, err
		}
	}
	t.Stop()

	return res, nil
}

func (f *Active2) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	ss, release := f.indexes.Snapshot()
	defer release()

	if ss.info.DocsTotal == 0 { // it is empty active fraction state
		metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
		return &seq.QPR{Aggs: make([]seq.AggregatableSamples, len(params.AggQ))}, nil
	}

	aggLimits := processor.AggLimits(f.Config.Search.AggLimits)

	// Limit the parameter range to data boundaries to prevent histogram overflow
	params.From = max(params.From, ss.info.From)
	params.To = min(params.To, ss.info.To)

	sw := stopwatch.New()
	defer sw.Export(getActiveSearchMetric(params))

	t := sw.Start("total")
	qprs := make([]*seq.QPR, 0, len(ss.indexes))
	for _, index := range ss.indexes {
		si := searchIndex{ctx: ctx, index: index}
		qpr, err := processor.IndexSearch(ctx, params, &si, aggLimits, sw)
		if err != nil {
			return nil, err
		}
		qprs = append(qprs, qpr)
	}
	res := processor.MergeQPRs(qprs, params)
	res.IDs.ApplyHint(ss.info.Name())
	t.Stop()

	return res, nil
}

func (f *Active2) Info() *frac.Info {
	return f.indexes.Info()
}

func (f *Active2) Contains(id seq.MID) bool {
	return f.Info().IsIntersecting(id, id)
}

func (f *Active2) IsIntersecting(from, to seq.MID) bool {
	return f.Info().IsIntersecting(from, to)
}

func (f *Active2) Release() {
	f.releaseMem()

	if !f.Config.KeepMetaFile {
		util.RemoveFile(f.metaFile.Name())
	}

	if !f.Config.SkipSortDocs {
		// we use sorted docs in sealed fraction so we can remove original docs of active fraction
		util.RemoveFile(f.docsFile.Name())
	}

}

func (f *Active2) Suicide() {
	f.releaseMem()

	util.RemoveFile(f.metaFile.Name())
	util.RemoveFile(f.docsFile.Name())
	util.RemoveFile(f.BaseFileName + consts.SdocsFileSuffix)
}

func (f *Active2) releaseMem() {
	f.writer.Stop()
	f.merger.Stop()
	f.indexes.Release()

	f.docsCache.Release()
	f.sortCache.Release()

	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
	if err := f.docsFile.Close(); err != nil {
		logger.Error("can't close docs file", zap.String("frac", f.BaseFileName), zap.Error(err))
	}
}
