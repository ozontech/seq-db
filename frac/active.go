package frac

import (
	"context"
	"errors"
	"fmt"
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
	sealed   Fraction // derivative fraction
	suicided bool

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
	indexWg sync.WaitGroup

	sealingMu sync.RWMutex
	isSealed  bool
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
// Checks the state of a faction and may return an error if the faction has already started sealing.
func (f *Active) Append(docs, metas []byte) (err error) {
	if !f.tryStartAppend() {
		return errors.New("fraction is not writable")
	}

	sw := stopwatch.New()
	m := sw.Start("append")
	if err = f.writer.Write(docs, metas, sw); err != nil {
		m.Stop()
		return err
	}
	f.updateDiskStats(uint64(len(docs)), uint64(len(metas)))
	f.indexer.Index(f, metas, &f.indexWg, sw)
	m.Stop()
	sw.Export(bulkStagesSeconds)

	return nil
}

func (f *Active) tryStartAppend() bool {
	f.sealingMu.RLock()
	defer f.sealingMu.RUnlock()

	if f.isSealed {
		return false
	}

	f.indexWg.Add(1) // It is important to place it inside the lock
	return true
}

func (f *Active) WaitWriteIdle() {
	f.indexWg.Wait()
}

func (f *Active) setSealed() error {
	f.sealingMu.Lock()
	defer f.sealingMu.Unlock()

	if f.isSealed {
		return errors.New("fraction is already sealed")
	}
	f.isSealed = true
	return nil
}

func (f *Active) Seal(params SealParams) (*PreloadedData, error) {
	if err := f.setSealed(); err != nil {
		return nil, err
	}

	logger.Info("waiting fraction to stop write...")
	start := time.Now()
	f.WaitWriteIdle()
	logger.Info("write is stopped", zap.Float64("time_wait_s", util.DurationToUnit(time.Since(start), "s")))

	return seal(f, params)
}

func (f *Active) GetAllDocuments() []uint32 {
	return f.TokenList.GetAllTokenLIDs().GetLIDs(f.MIDs, f.RIDs)
}

func (f *Active) Release(sealed Fraction) error {
	f.useMu.Lock()
	f.sealed = sealed
	f.useMu.Unlock()

	f.TokenList.Stop()

	f.RIDs = nil
	f.MIDs = nil
	f.TokenList = nil
	f.DocsPositions = nil

	// Once frac is released, it is safe to remove the docs file,
	// since search queries will use the sealed implementation.

	if !f.Config.SkipSortDocs {
		if err := f.docsFile.Close(); err != nil {
			return fmt.Errorf("closing docs file: %w", err)
		}
		rmFileName := f.BaseFileName + consts.DocsFileSuffix
		if err := os.Remove(rmFileName); err != nil {
			logger.Error("error removing docs file",
				zap.String("file", rmFileName),
				zap.Error(err))
		}
	}

	f.docsCache.Release()
	f.sortCache.Release()

	if !f.Config.KeepMetaFile {
		rmFileName := f.BaseFileName + consts.MetaFileSuffix
		if err := os.Remove(rmFileName); err != nil {
			logger.Error("can't delete metas file", zap.String("file", rmFileName), zap.Error(err))
		}
	}

	return nil
}

func (f *Active) updateDiskStats(docsLen, metaLen uint64) {
	f.infoMu.Lock()
	f.info.DocsOnDisk += docsLen
	f.info.MetaOnDisk += metaLen
	f.infoMu.Unlock()
}

func (f *Active) close(closeDocs bool, hint string) {
	f.writer.Stop()
	if closeDocs {
		if err := f.docsFile.Close(); err != nil {
			logger.Error("can't close docs file",
				zap.String("frac", f.BaseFileName),
				zap.String("type", "active"),
				zap.String("hint", hint),
				zap.Error(err))
		}
	}

	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file",
			zap.String("frac", f.BaseFileName),
			zap.String("type", "active"),
			zap.String("hint", hint),
			zap.Error(err))
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

func (f *Active) Suicide() { // it seams we never call this method (of Active fraction)
	f.useMu.Lock()
	f.suicided = true
	f.useMu.Unlock()

	if !f.isSealed {
		f.close(true, "suicide")

		err := os.Remove(f.BaseFileName + consts.MetaFileSuffix)
		if err != nil {
			logger.Error("error removing file",
				zap.String("file", f.BaseFileName+consts.MetaFileSuffix),
				zap.Error(err),
			)
		}
	}

	rmPath := f.BaseFileName + consts.DocsFileSuffix
	if err := os.Remove(rmPath); err != nil {
		logger.Error("error removing file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}

	rmPath = f.BaseFileName + consts.SdocsFileSuffix
	if err := os.Remove(rmPath); err != nil && !os.IsNotExist(err) {
		logger.Error("error removing file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}

	if f.isSealed {
		rmPath = f.BaseFileName + consts.IndexFileSuffix
		if err := os.Remove(rmPath); err != nil {
			logger.Error("error removing file",
				zap.String("file", rmPath),
				zap.Error(err),
			)
		}
	}
}

func (f *Active) String() string {
	return fracToString(f, "active")
}

func (f *Active) DataProvider(ctx context.Context) (DataProvider, func()) {
	f.useMu.RLock()

	if f.sealed == nil && !f.suicided && f.Info().DocsTotal > 0 { // it is ordinary active fraction state
		dp := f.createDataProvider(ctx)
		return dp, func() {
			dp.release()
			f.useMu.RUnlock()
		}
	}

	defer f.useMu.RUnlock()

	if f.sealed != nil { // move on to the daughter sealed faction
		dp, releaseSealed := f.sealed.DataProvider(ctx)
		metric.CountersTotal.WithLabelValues("use_sealed_from_active").Inc()
		return dp, releaseSealed
	}

	if f.suicided {
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
	}

	return EmptyDataProvider{}, func() {}
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
