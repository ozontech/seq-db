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

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/tracer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type ActiveIDsProvider struct {
	mids     []uint64
	rids     []uint64
	inverser *inverser
}

func (p *ActiveIDsProvider) GetMID(lid seq.LID) seq.MID {
	restoredLID := p.inverser.Revert(uint32(lid))
	return seq.MID(p.mids[restoredLID])
}

func (p *ActiveIDsProvider) GetRID(lid seq.LID) seq.RID {
	restoredLID := p.inverser.Revert(uint32(lid))
	return seq.RID(p.rids[restoredLID])
}

func (p *ActiveIDsProvider) Len() int {
	return p.inverser.Len()
}

func (p *ActiveIDsProvider) LessOrEqual(lid seq.LID, id seq.ID) bool {
	checkedMID := p.GetMID(lid)
	if checkedMID == id.MID {
		return p.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

type ActiveDataProvider struct {
	*Active
	sc       *SearchCell
	tracer   *tracer.Tracer
	inverser *inverser
}

func (dp *ActiveDataProvider) Tracer() *tracer.Tracer {
	return dp.tracer
}

func (dp *ActiveDataProvider) IDsProvider(_, _ *UnpackCache) IDsProvider {
	return &ActiveIDsProvider{
		mids:     dp.MIDs.GetVals(),
		rids:     dp.RIDs.GetVals(),
		inverser: dp.inverser,
	}
}

func (dp *ActiveDataProvider) GetTIDsByTokenExpr(t parser.Token, tids []uint32) ([]uint32, error) {
	return dp.Active.GetTIDsByTokenExpr(dp.sc, t, tids, dp.tracer)
}

func (dp *ActiveDataProvider) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	return dp.Active.GetLIDsFromTIDs(tids, dp.inverser, stats, minLID, maxLID, dp.tracer, order)
}

func (dp *ActiveDataProvider) Fetch(id seq.ID, docsBuf []byte, _, _ *UnpackCache) ([]byte, []byte, error) {
	return dp.Active.Fetch(id, docsBuf)
}

type Active struct {
	frac

	MIDs *UInt64s
	RIDs *UInt64s

	TokenList     *TokenList
	DocsPositions *DocsPositions

	metasFile *os.File

	appendQueueSize atomic.Uint32
	appender        ActiveAppender

	sealingMu        sync.RWMutex
	isSealed         bool
	shouldRemoveMeta bool

	// to transfer data to sealed frac
	lidsTable  *lids.Table
	tokenTable token.Table
	sealedIDs  *SealedIDs

	// derivative fraction
	sealed Fraction
}

func NewActive(baseFileName string, metaRemove bool, indexWorkers *IndexWorkers, reader *disk.Reader, docBlockCache *cache.Cache[[]byte]) *Active {
	mids := NewIDs()
	rids := NewIDs()

	creationTime := uint64(time.Now().UnixMilli())

	f := &Active{
		shouldRemoveMeta: metaRemove,
		TokenList:        NewActiveTokenList(conf.IndexWorkers),
		DocsPositions:    NewSyncDocsPositions(),
		MIDs:             mids,
		RIDs:             rids,
		frac: frac{
			docBlockCache: docBlockCache,
			BaseFileName:  baseFileName,
			DocBlocks:     NewIDs(),
			reader:        reader,
			info: &Info{
				Ver:                   buildinfo.Version,
				Path:                  baseFileName,
				From:                  math.MaxUint64,
				To:                    0,
				CreationTime:          creationTime,
				ConstIDsPerBlock:      consts.IDsPerBlock,
				ConstRegularBlockSize: consts.RegularBlockSize,
				ConstLIDBlockCap:      consts.LIDBlockCap,
			},
		},
	}

	// use of 0 as keys in maps is prohibited – it's system key, so add first element
	f.MIDs.Append(math.MaxUint64)
	f.RIDs.Append(math.MaxUint64)

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	file, err := os.OpenFile(f.BaseFileName+consts.DocsFileSuffix, os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't create docs file", zap.Error(err))
		panic("_")
	}
	f.docsFile = file

	file, err = os.OpenFile(f.BaseFileName+consts.MetaFileSuffix, os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't create meta file", zap.Error(err))
		panic("_")
	}
	f.metasFile = file

	stat, err := f.docsFile.Stat()
	if err != nil {
		logger.Fatal("can't stat docs file",
			zap.String("file", f.docsFile.Name()),
			zap.Error(err),
		)
		panic("_")
	}
	f.info.DocsOnDisk = uint64(stat.Size())

	stat, err = f.metasFile.Stat()
	if err != nil {
		logger.Fatal("can't stat metas file",
			zap.String("file", f.metasFile.Name()),
			zap.Error(err),
		)
		panic("_")
	}
	f.info.MetaOnDisk = uint64(stat.Size())

	f.appender = StartAppender(f.docsFile, f.metasFile, conf.IndexWorkers, conf.SkipFsync, indexWorkers)

	return f
}

func (f *Active) ReplayBlocks(ctx context.Context) error {
	logger.Info("start replaying...")

	if _, err := f.docsFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("can't seek docs file: filename=%s, err=%w", f.docsFile.Name(), err)
	}
	if _, err := f.metasFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("can't seek metas file: filename=%s, err=%w", f.metasFile.Name(), err)
	}

	targetSize := f.info.MetaOnDisk
	t := time.Now()

	reader := disk.NewReader(metric.StoreBytesRead)
	defer reader.Stop()

	f.info.DocsOnDisk = 0
	f.info.MetaOnDisk = 0
	docsPos := uint64(0)
	metaPos := uint64(0)
	step := targetSize / 10
	next := step
	var outBuf []byte
out:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			readTask := reader.ReadDocBlock(f.metasFile, int64(metaPos), 0, outBuf)
			outBuf = readTask.Buf
			if readTask.Err == io.EOF {
				if readTask.N != 0 {
					logger.Warn("last meta block is partially written, skipping it")
				}
				break out
			}
			if readTask.Err != nil && readTask.Err != io.EOF {
				return readTask.Err
			}
			result := append([]byte{}, readTask.Buf...)

			if metaPos > next {
				next += step
				progress := float64(metaPos) / float64(targetSize) * 100
				logger.Info("replaying batch, meta",
					zap.Uint64("from", metaPos),
					zap.Uint64("to", metaPos+readTask.N),
					zap.Uint64("target", targetSize),
					util.ZapFloat64WithPrec("progress_percentage", progress, 2),
				)
			}

			docBlockLen := disk.DocBlock(result).GetExt1()
			docsPos += docBlockLen
			metaPos += readTask.N

			if err := f.Replay(docBlockLen, result); err != nil {
				return err
			}
		}
	}

	f.WaitWriteIdle()

	if _, err := f.docsFile.Seek(int64(docsPos), io.SeekStart); err != nil {
		return fmt.Errorf("can't seek docs file: file=%s, err=%w", f.docsFile.Name(), err)
	}
	if _, err := f.metasFile.Seek(int64(metaPos), io.SeekStart); err != nil {
		return fmt.Errorf("can't seek meta file: file=%s, err=%w", f.metasFile.Name(), err)
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

func (f *Active) Seal() error {
	if err := f.setSealed(); err != nil {
		return err
	}

	logger.Info("waiting fraction to stop write...")
	start := time.Now()
	f.WaitWriteIdle()
	logger.Info("write is stopped", zap.Float64("time_wait_s", util.DurationToUnit(time.Since(start), "s")))

	seal(f)
	return nil
}

func (f *Active) Fetch(id seq.ID, docsBuf []byte) ([]byte, []byte, error) {
	docPos := f.DocsPositions.GetSync(id)

	if docPos == DocPosNotFound {
		return nil, docsBuf, nil
	}

	docBlockIndex, docOffset := docPos.Unpack()

	vals := f.DocBlocks.GetVals()
	pos := vals[docBlockIndex]

	return f.readDoc(pos, 0, docOffset, docsBuf)
}

func (f *Active) GetAllDocuments() []uint32 {
	return f.TokenList.GetAllTokenLIDs().GetLIDs(f.MIDs, f.RIDs)
}

func (f *Active) GetTIDsByTokenExpr(sc *SearchCell, tk parser.Token, tids []uint32, tr *tracer.Tracer) ([]uint32, error) {
	res, err := f.TokenList.FindPattern(sc.Context, tk, tids, tr)
	return res, err
}

func (f *Active) GetLIDsFromTIDs(tids []uint32, inv *inverser, _ lids.Counter, minLID, maxLID uint32, tr *tracer.Tracer, order seq.DocsOrder) []node.Node {
	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		m := tr.Start("provide")
		tlids := f.TokenList.Provide(tid)
		m.Stop()

		m = tr.Start("LIDs")
		unmapped := tlids.GetLIDs(f.MIDs, f.RIDs)
		m.Stop()

		m = tr.Start("inverse")
		inverse := inverseLIDs(unmapped, inv, minLID, maxLID)
		nodes = append(nodes, node.NewStatic(inverse, order.IsReverse()))
		m.Stop()
	}
	return nodes
}

func inverseLIDs(unmapped []uint32, inv *inverser, minLID, maxLID uint32) []uint32 {
	result := make([]uint32, 0, len(unmapped))
	for _, v := range unmapped {
		// we skip those values that are not in the inverser, because such values appeared after the search query started
		if val, ok := inv.Inverse(v); ok {
			if minLID <= uint32(val) && uint32(val) <= maxLID {
				result = append(result, uint32(val))
			}
		}
	}
	return result
}

func (f *Active) IDsProvider(_ *SearchCell, _, _ *UnpackCache, inv *inverser) IDsProvider {
	return &ActiveIDsProvider{
		mids:     f.MIDs.GetVals(),
		rids:     f.RIDs.GetVals(),
		inverser: inv,
	}
}

func (f *Active) inverser(tr *tracer.Tracer) *inverser {
	m := tr.Start("get_all_documents")
	mapping := f.GetAllDocuments()
	m.Stop()

	if len(mapping) == 0 {
		return nil
	}

	m = tr.Start("inverse")
	i := newInverser(mapping)
	m.Stop()

	return i
}

func (f *Active) GetValByTID(tid uint32) []byte {
	return f.TokenList.GetValByTID(tid)
}

func (f *Active) Type() string {
	return TypeActive
}

func (f *Active) Release(sealed Fraction) {
	f.useLock.Lock()
	defer f.useLock.Unlock()

	f.sealed = sealed

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
		err := f.docsFile.Close()
		if err != nil {
			logger.Error("can't close docs file",
				f.closeLogArgs("active", hint, err)...,
			)
		}
	}

	err := f.metasFile.Close()
	if err != nil {
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

func (f *Active) Suicide() {
	f.useLock.Lock()
	defer f.useLock.Unlock()

	f.suicided = true

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

	err := os.Remove(f.BaseFileName + consts.DocsFileSuffix)
	if err != nil {
		logger.Error("error removing file",
			zap.String("file", f.BaseFileName+consts.DocsFileSuffix),
			zap.Error(err),
		)
	}

	if f.isSealed {
		err := os.Remove(f.BaseFileName + consts.IndexFileSuffix)
		if err != nil {
			logger.Error("error removing file",
				zap.String("file", f.BaseFileName+consts.IndexFileSuffix),
				zap.Error(err),
			)
		}
	}
}

func (f *Active) FullSize() uint64 {
	stats := f.Info()
	return stats.DocsOnDisk + stats.MetaOnDisk
}

func (f *Active) ExplainDoc(_ seq.ID) {

}

func (f *Active) String() string {
	return f.toString("active")
}

func (f *Active) DataProvider(ctx context.Context) (DataProvider, func(), bool) {
	f.useLock.RLock()
	if f.sealed != nil {
		defer f.useLock.RUnlock()
		dp, releaseSealed, ok := f.sealed.DataProvider(ctx)
		metric.CountersTotal.WithLabelValues("use_sealed_from_active").Inc()
		return dp, releaseSealed, ok
	}

	tr := tracer.New()
	inverser := f.inverser(tr)

	if inverser == nil {
		f.useLock.RUnlock()
		return nil, nil, false
	}

	dp := ActiveDataProvider{
		Active:   f,
		sc:       NewSearchCell(ctx),
		tracer:   tr,
		inverser: inverser,
	}

	return &dp, func() {
		inverser.Release()
		f.useLock.RUnlock()
	}, true
}
