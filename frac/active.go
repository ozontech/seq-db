package frac

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Active struct {
	Config Config

	BaseFileName string

	useMu    sync.RWMutex
	sealed   Fraction // derivative fraction
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
	docsCache  *cache.Cache[[]byte]
	docsReader disk.DocsReader

	metaFile   *os.File
	metaReader disk.DocBlocksReader

	writer  *ActiveWriter
	indexer *ActiveIndexer
}

func NewActive(
	baseFileName string,
	indexWorkers *IndexWorkers,
	readLimiter *disk.ReadLimiter,
	docsCache *cache.Cache[[]byte],
	config Config,
) *Active {
	docsFile, docsStats := openFile(baseFileName + consts.DocsFileSuffix)
	metaFile, metaStats := openFile(baseFileName + consts.MetaFileSuffix)

	f := &Active{
		TokenList:     NewActiveTokenList(conf.IndexWorkers),
		DocsPositions: NewSyncDocsPositions(),
		MIDs:          NewIDs(),
		RIDs:          NewIDs(),
		DocBlocks:     NewIDs(),

		docsFile:   docsFile,
		docsReader: disk.NewDocsReader(disk.NewDocBlocksReader(readLimiter, docsFile), docsCache),
		docsCache:  docsCache,

		metaFile:   metaFile,
		metaReader: disk.NewDocBlocksReader(readLimiter, metaFile),

		indexer: NewActiveIndexer(indexWorkers),
		writer:  NewActiveWriter(docsFile, metaFile, docsStats.Size(), conf.SkipFsync),

		BaseFileName: baseFileName,
		info:         NewInfo(baseFileName, uint64(docsStats.Size()), uint64(metaStats.Size())),
		Config:       config,
	}

	// use of 0 as keys in maps is prohibited – it's system key, so add first element
	f.MIDs.Append(math.MaxUint64)
	f.RIDs.Append(math.MaxUint64)

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	return f
}

func openFile(name string) (*os.File, os.FileInfo) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't create docs file", zap.String("file", name), zap.Error(err))
	}
	stat, err := file.Stat()
	if err != nil {
		logger.Fatal("can't stat docs file", zap.String("file", name), zap.Error(err))
	}
	return file, stat
}

func (f *Active) ReplayBlocks(ctx context.Context) error {
	logger.Info("start replaying...")

	f.info.DocsOnDisk = 0
	f.info.MetaOnDisk = 0
	docsPos := uint64(0)
	metaPos := uint64(0)

	targetSize := f.info.MetaOnDisk
	step := targetSize / 10
	next := step

	sw := stopwatch.New()
	wg := sync.WaitGroup{}

	t := time.Now()
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
			if err != nil {
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

			docsPos += disk.DocBlock(meta).GetExt1()
			disk.DocBlock(meta).SetExt2(docsPos) // todo: can remove it after transition period
			metaPos += metaSize

			wg.Add(1)
			f.indexer.Index(f, meta, &wg, sw)
		}
	}

	if _, err := f.docsFile.Seek(int64(docsPos), io.SeekStart); err != nil {
		return fmt.Errorf("can't seek file: %s, err=%w", f.docsFile.Name(), err)
	}
	if _, err := f.metaFile.Seek(int64(metaPos), io.SeekStart); err != nil {
		return fmt.Errorf("can't seek file: %s, err=%w", f.metaFile.Name(), err)
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

// Append causes data to be written on disk and sends metas to index workers
func (f *Active) Append(docs, metas []byte, wg *sync.WaitGroup) (err error) {
	sw := stopwatch.New()

	m := sw.Start("append")

	if err = f.writer.Write(docs, metas, sw); err != nil {
		m.Stop()
		return err
	}

	f.UpdateDiskStats(uint64(len(docs)), uint64(len(metas)))
	f.indexer.Index(f, metas, wg, sw)

	m.Stop()

	sw.Export(metric.BulkStagesSeconds)

	return nil
}

func (f *Active) GetAllDocuments() []uint32 {
	return f.TokenList.GetAllTokenLIDs().GetLIDs(f.MIDs, f.RIDs)
}

func (f *Active) Release(forceRemoveMeta bool) {
	f.useMu.Lock()
	if f.released {
		f.useMu.Unlock()
		return
	}
	f.released = true
	f.useMu.Unlock()

	f.writer.Stop()
	f.TokenList.Stop()

	f.RIDs = nil
	f.MIDs = nil
	f.TokenList = nil
	f.DocsPositions = nil

	f.releaseMeta(forceRemoveMeta || !f.Config.KeepMetaFile)
}

func (f *Active) UpdateDiskStats(docsLen, metaLen uint64) {
	f.infoMu.Lock()
	f.info.DocsOnDisk += docsLen
	f.info.MetaOnDisk += metaLen
	f.infoMu.Unlock()
}

func (f *Active) releaseMeta(delFile bool) {
	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file",
			zap.String("frac", f.BaseFileName),
			zap.String("type", "active"),
			zap.Error(err))
	}

	if delFile {
		rmFileName := f.BaseFileName + consts.MetaFileSuffix
		if err := os.Remove(rmFileName); err != nil {
			logger.Error("can't delete metas file", zap.String("file", rmFileName), zap.Error(err))
		}
	}
}

func (f *Active) removeDocs() {
	if err := f.docsFile.Close(); err != nil {
		logger.Error("can't close docs file",
			zap.String("frac", f.BaseFileName),
			zap.String("type", "active"),
			zap.Error(err))
	}

	rmPath := f.BaseFileName + consts.DocsFileSuffix
	if err := os.Remove(rmPath); err != nil {
		logger.Error("error removing file", zap.String("file", rmPath), zap.Error(err))
	}

	rmPath = f.BaseFileName + consts.SdocsFileSuffix
	if err := os.Remove(rmPath); err != nil {
		logger.Error("error removing file", zap.String("file", rmPath), zap.Error(err))
	}

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

func (f *Active) buildInfoDistribution(ids []seq.ID) {
	// We must update `info` inside active fraction because we are passing this `info`
	// with built `distribution` to derivative sealed fraction.
	// In fact we must not call this method concurrently so no one else should wait for Lock().
	f.infoMu.Lock()
	f.info.BuildDistribution(ids)
	f.infoMu.Unlock()
}

func (f *Active) Suicide() {
	f.useMu.Lock()
	f.suicided = true
	f.useMu.Unlock()

	f.Release(true)
	f.removeDocs()
}

func (f *Active) String() string {
	return fracToString(f, "active")
}

func (f *Active) DataProvider(ctx context.Context) (DataProvider, func()) {
	f.useMu.RLock()

	if !f.suicided && !f.released && f.Info().DocsTotal > 0 { // it is ordinary active fraction state
		dp := f.createDataProvider(ctx)
		return dp, func() {
			dp.release()
			f.useMu.RUnlock()
		}
	}

	defer f.useMu.RUnlock()

	if f.suicided {
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
	}

	return EmptyDataProvider{}, func() {}
}

func (f *Active) createDataProvider(ctx context.Context) *activeDataProvider {
	return &activeDataProvider{
		ctx:    ctx,
		config: &f.Config,
		info:   f.Info(),

		mids:      f.MIDs,
		rids:      f.RIDs,
		tokenList: f.TokenList,

		blocksOffsets: f.DocBlocks.GetVals(),
		docsPositions: f.DocsPositions,
		docsReader:    &f.docsReader,
	}
}

func (f *Active) setInfoSealingTime(newTime uint64) {
	f.infoMu.Lock()
	f.info.SealingTime = newTime
	f.infoMu.Unlock()
}

func (f *Active) setInfoIndexOnDisk(newSize uint64) {
	f.infoMu.Lock()
	f.info.IndexOnDisk = newSize
	f.infoMu.Unlock()
}

func (f *Active) Info() *Info {
	f.infoMu.RLock()
	defer f.infoMu.RUnlock()

	return &(*f.info)
}

func (f *Active) Contains(id seq.MID) bool {
	return f.Info().IsIntersecting(id, id)
}

func (f *Active) IsIntersecting(from, to seq.MID) bool {
	return f.Info().IsIntersecting(from, to)
}
