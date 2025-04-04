package frac

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Active struct {
	frac

	MIDs *UInt64s
	RIDs *UInt64s

	DocBlocks *UInt64s

	TokenList *TokenList

	DocsPositions *DocsPositions

	docsReader *disk.DocsReader
	docsCache  *cache.Cache[[]byte]

	docsFile *os.File
	metaFile *os.File

	appendQueueSize atomic.Uint32
	appender        ActiveAppender

	sealingMu        sync.RWMutex
	isSealed         bool
	shouldRemoveMeta bool

	// to transfer data to sealed frac
	idsTable   IDsTable
	lidsTable  *lids.Table
	tokenTable token.Table

	// derivative fraction
	sealed Fraction
}

func NewActive(
	baseFileName string,
	metaRemove bool,
	indexWorkers *IndexWorkers,
	readLimiter *disk.ReadLimiter,
	docsCache *cache.Cache[[]byte],
	config Config,
) *Active {
	docsFile, docsStats := openFile(baseFileName + consts.DocsFileSuffix)
	metaFile, metaStats := openFile(baseFileName + consts.MetaFileSuffix)

	f := &Active{
		shouldRemoveMeta: metaRemove,
		TokenList:        NewActiveTokenList(conf.IndexWorkers),
		DocsPositions:    NewSyncDocsPositions(),
		MIDs:             NewIDs(),
		RIDs:             NewIDs(),
		DocBlocks:        NewIDs(),

		docsFile:   docsFile,
		metaFile:   metaFile,
		docsReader: disk.NewDocsReader(readLimiter, docsFile, docsCache),
		docsCache:  docsCache,

		appender: StartAppender(docsFile, metaFile, conf.IndexWorkers, conf.SkipFsync, indexWorkers),

		frac: frac{
			BaseFileName: baseFileName,
			info:         NewInfo(baseFileName, uint64(docsStats.Size()), uint64(metaStats.Size())),
			Config:       config,
		},
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

func (f *Active) ReplayBlocks(ctx context.Context, reader *disk.ReadLimiter) error {
	logger.Info("start replaying...")

	if _, err := f.docsFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("can't seek docs file: filename=%s, err=%w", f.docsFile.Name(), err)
	}
	if _, err := f.metaFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("can't seek metas file: filename=%s, err=%w", f.metaFile.Name(), err)
	}

	targetSize := f.info.MetaOnDisk
	t := time.Now()

	metaReader := disk.NewDocsReader(reader, f.metaFile, nil)

	f.info.DocsOnDisk = 0
	f.info.MetaOnDisk = 0
	docsPos := uint64(0)
	metaPos := uint64(0)
	step := targetSize / 10
	next := step
out:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			result, n, err := metaReader.ReadDocBlock(int64(metaPos))
			if err == io.EOF {
				if n != 0 {
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
					zap.Uint64("to", metaPos+n),
					zap.Uint64("target", targetSize),
					util.ZapFloat64WithPrec("progress_percentage", progress, 2),
				)
			}

			docBlockLen := disk.DocBlock(result).GetExt1()
			docsPos += docBlockLen
			metaPos += n

			if err := f.Replay(docBlockLen, result); err != nil {
				return err
			}
		}
	}

	f.WaitWriteIdle()

	if _, err := f.docsFile.Seek(int64(docsPos), io.SeekStart); err != nil {
		return fmt.Errorf("can't seek docs file: file=%s, err=%w", f.docsFile.Name(), err)
	}
	if _, err := f.metaFile.Seek(int64(metaPos), io.SeekStart); err != nil {
		return fmt.Errorf("can't seek meta file: file=%s, err=%w", f.metaFile.Name(), err)
	}

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

// Append causes data to be written on disk and sends IndexTask to index workers
// Checks the state of a faction and may return an error if the faction has already started sealing.
func (f *Active) Append(docs, metas []byte, writeQueue *atomic.Uint64) error {
	if !f.incAppendQueueSize() {
		return errors.New("fraction is not writable")
	}

	f.appender.In(f, docs, metas, writeQueue, &f.appendQueueSize)

	return nil
}

func (f *Active) Replay(docsLen uint64, metas []byte) error {
	if !f.incAppendQueueSize() {
		// shouldn't actually be possible
		return errors.New("replaying of fraction being sealed")
	}

	f.appender.InReplay(f, docsLen, metas, &f.appendQueueSize)

	return nil
}

func (f *Active) incAppendQueueSize() bool {
	f.sealingMu.RLock()
	defer f.sealingMu.RUnlock()

	if f.isSealed {
		return false
	}

	f.appendQueueSize.Inc()
	return true
}

func (f *Active) WaitWriteIdle() {
	for f.appendQueueSize.Load() > 0 {
		time.Sleep(time.Millisecond * 10)
	}
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

func (f *Active) Seal(params SealParams) (*os.File, error) {
	if err := f.setSealed(); err != nil {
		return nil, err
	}

	logger.Info("waiting fraction to stop write...")
	start := time.Now()
	f.WaitWriteIdle()
	logger.Info("write is stopped", zap.Float64("time_wait_s", util.DurationToUnit(time.Since(start), "s")))

	return seal(f, params), nil
}

func (f *Active) GetAllDocuments() []uint32 {
	return f.TokenList.GetAllTokenLIDs().GetLIDs(f.MIDs, f.RIDs)
}

func (f *Active) Release(sealed Fraction) {
	f.useLock.Lock()
	f.sealed = sealed
	f.useLock.Unlock()

	f.TokenList.Stop()

	f.RIDs = nil
	f.MIDs = nil
	f.TokenList = nil
	f.DocsPositions = nil
}

func (f *Active) UpdateDiskStats(docsLen, metaLen uint64) uint64 {
	f.statsMu.Lock()
	pos := f.info.DocsOnDisk
	f.info.DocsOnDisk += docsLen
	f.info.MetaOnDisk += metaLen
	f.statsMu.Unlock()
	return pos
}

func (f *Active) close(closeDocs bool, hint string) {
	f.appender.Stop()
	if closeDocs {
		if err := f.docsFile.Close(); err != nil {
			logger.Error("can't close docs file",
				f.closeLogArgs("active", hint, err)...,
			)
		}
	}

	if err := f.metaFile.Close(); err != nil {
		logger.Error("can't close meta file",
			f.closeLogArgs("active", hint, err)...,
		)
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

func (f *Active) AppendID(id seq.ID) uint32 {
	return f.AppendIDs([]seq.ID{id})[0]
}

func (f *Active) UpdateStats(minMID, maxMID seq.MID, docCount uint32, sizeCount uint64) {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	if f.info.From > minMID {
		f.info.From = minMID
	}
	if f.info.To < maxMID {
		f.info.To = maxMID
	}
	f.info.DocsTotal += docCount
	f.info.DocsRaw += sizeCount
}

func (f *Active) BuildInfoDistribution(ids []seq.ID) {
	info := f.Info()
	info.BuildDistribution(ids)

	f.statsMu.Lock()
	f.info = info
	f.statsMu.Unlock()
}

func (f *Active) Suicide() { // it seams we never call this method (of Active fraction)
	f.useLock.Lock()
	f.suicided = true
	f.useLock.Unlock()

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
	if err := os.Remove(rmPath); err != nil {
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
	return f.toString("active")
}

func (f *Active) DataProvider(ctx context.Context) (DataProvider, func()) {
	f.useLock.RLock()

	if f.sealed == nil && !f.suicided && f.Info().DocsTotal > 0 { // it is ordinary active fraction state
		dp := f.createDataProvider(ctx)
		return dp, func() {
			dp.release()
			f.useLock.RUnlock()
		}
	}

	defer f.useLock.RUnlock()

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
		config: &f.Config,
		info:   f.Info(),

		mids:      f.MIDs,
		rids:      f.RIDs,
		tokenList: f.TokenList,

		blocksOffsets: f.DocBlocks.GetVals(),
		docsPositions: f.DocsPositions,
		docsReader:    f.docsReader,
	}
}
