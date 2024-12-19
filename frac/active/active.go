package active

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
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
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
	sc       *frac.SearchCell
	tracer   *tracer.Tracer
	inverser *inverser
}

// getInverser creates on demand and returns inverser
// inverser creation is expensive operation
func (dp *ActiveDataProvider) getInverser() *inverser {
	if dp.inverser == nil {
		dp.inverser = dp.Active.inverser(dp.tracer)
	}
	return dp.inverser
}

func (dp *ActiveDataProvider) Tracer() *tracer.Tracer {
	return dp.tracer
}

func (dp *ActiveDataProvider) IDsProvider() frac.IDsProvider {
	return &ActiveIDsProvider{
		mids:     dp.mids.GetVals(),
		rids:     dp.rids.GetVals(),
		inverser: dp.getInverser(),
	}
}

func (dp *ActiveDataProvider) GetTIDsByTokenExpr(t parser.Token, tids []uint32) ([]uint32, error) {
	return dp.Active.GetTIDsByTokenExpr(dp.sc, t, tids, dp.tracer)
}

func (dp *ActiveDataProvider) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	return dp.Active.GetLIDsFromTIDs(tids, dp.getInverser(), stats, minLID, maxLID, dp.tracer, order)
}

func (dp *ActiveDataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	defer dp.tracer.UpdateMetric(metric.FetchActiveStagesSeconds)

	m := dp.tracer.Start("get_doc_params_by_id")
	docsPos := make([]frac.DocPos, len(ids))
	for i, id := range ids {
		docsPos[i] = dp.Active.docsPositions.GetSync(id)
	}
	m.Stop()

	m = dp.tracer.Start("unpack_offsets")
	blocks, offsets, index := frac.GroupDocsOffsets(docsPos)
	m.Stop()

	m = dp.tracer.Start("read_doc")
	res := make([][]byte, len(ids))
	blocksOffsets := dp.Active.docBlocks.GetVals()
	for i, docOffsets := range offsets {
		docs, err := dp.Base.DocsReader.Read(blocksOffsets[blocks[i]], docOffsets)
		if err != nil {
			return nil, err
		}
		for i, j := range index[i] {
			res[j] = docs[i]
		}
	}
	m.Stop()

	return res, nil
}

type Active struct {
	frac.Base

	infoMu sync.Mutex

	mids *UInt64s
	rids *UInt64s

	docBlocks *UInt64s

	tokenList *TokenList

	docsPositions *DocsPositions

	docsFile *os.File
	metaFile *os.File

	appendQueueSize atomic.Uint32
	appender        Appender

	sealingMu        sync.RWMutex
	isSealed         bool
	shouldRemoveMeta bool

	// to transfer data to sealed frac
	lidsTable  *lids.Table
	tokenTable token.Table
	sealedIDs  *sealed.IDs

	// derivative fraction
	sealed frac.Fraction
}

func openFile(f string) (*os.File, int64) {
	file, err := os.OpenFile(f, os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't create file", zap.Error(err))
	}

	stat, err := file.Stat()
	if err != nil {
		logger.Fatal("can't stat file", zap.String("file", file.Name()), zap.Error(err))
	}

	return file, stat.Size()
}

func NewActive(baseFileName string, metaRemove bool, indexWorkers *IndexWorkers, reader *disk.Reader, docBlockCache *cache.Cache[[]byte]) *Active {
	docsFile, docsOnDisk := openFile(baseFileName + consts.DocsFileSuffix)
	metaFile, metaOnDisk := openFile(baseFileName + consts.MetaFileSuffix)

	active := &Active{
		shouldRemoveMeta: metaRemove,
		tokenList:        NewActiveTokenList(conf.IndexWorkers),
		docsPositions:    NewSyncDocsPositions(),
		mids:             NewIDs(),
		rids:             NewIDs(),
		docBlocks:        NewIDs(),

		docsFile: docsFile,
		metaFile: metaFile,

		Base: frac.Base{
			Info:         frac.NewInfo(time.Now(), baseFileName),
			DocsReader:   frac.NewDocsReader(docsFile, reader, docBlockCache),
			BaseFileName: baseFileName,
		},
	}

	active.Base.Info.DocsOnDisk = uint64(docsOnDisk)
	active.Base.Info.MetaOnDisk = uint64(metaOnDisk)

	// use of 0 as keys in maps is prohibited – it's system key, so add first element
	active.mids.Append(math.MaxUint64)
	active.rids.Append(math.MaxUint64)

	active.appender = StartAppender(active.docsFile, active.metaFile, conf.IndexWorkers, conf.SkipFsync, indexWorkers)

	logger.Info("active fraction created", zap.String("fraction", baseFileName))

	return active
}

func (f *Active) Info() frac.Info {
	f.infoMu.Lock()
	defer f.infoMu.Unlock()

	return f.Base.Info
}

func (f *Active) setInfoSealingTime(newTime uint64) {
	f.infoMu.Lock()
	defer f.infoMu.Unlock()

	f.Base.Info.SealingTime = newTime
}

func (f *Active) setInfoIndexOnDisk(newSize uint64) {
	f.infoMu.Lock()
	defer f.infoMu.Unlock()

	f.Base.Info.IndexOnDisk = newSize
}

func (f *Active) Contains(id seq.MID) bool {
	return f.IsIntersecting(id, id)
}

func (f *Active) IsIntersecting(from, to seq.MID) bool {
	return f.Info().IsIntersecting(from, to)
}

func (f *Active) ReplayBlocks(ctx context.Context) error {
	logger.Info("start replaying...")

	if _, err := f.docsFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("can't seek docs file: filename=%s, err=%w", f.docsFile.Name(), err)
	}
	if _, err := f.metaFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("can't seek metas file: filename=%s, err=%w", f.metaFile.Name(), err)
	}

	targetSize := f.Base.Info.MetaOnDisk
	t := time.Now()

	reader := disk.NewReader(metric.StoreBytesRead)
	defer reader.Stop()

	f.Base.Info.DocsOnDisk = 0
	f.Base.Info.MetaOnDisk = 0
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
			result, n, err := reader.ReadDocBlock(f.metaFile, int64(metaPos))
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
	throughputRaw := util.SizeToUnit(f.Base.Info.DocsRaw, "mb") / tookSeconds
	throughputMeta := util.SizeToUnit(f.Base.Info.MetaOnDisk, "mb") / tookSeconds
	logger.Info("active fraction replayed",
		zap.String("name", f.Base.Info.Name()),
		zap.Uint32("docs_total", f.Base.Info.DocsTotal),
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

func (f *Active) Seal(params SealParams) error {
	if err := f.setSealed(); err != nil {
		return err
	}

	logger.Info("waiting fraction to stop write...")
	start := time.Now()
	f.WaitWriteIdle()
	logger.Info("write is stopped", zap.Float64("time_wait_s", util.DurationToUnit(time.Since(start), "s")))

	seal(f, params)
	return nil
}

func (f *Active) GetAllDocuments() []uint32 {
	return f.tokenList.GetAllTokenLIDs().GetLIDs(f.mids, f.rids)
}

func (f *Active) GetTIDsByTokenExpr(sc *frac.SearchCell, tk parser.Token, tids []uint32, tr *tracer.Tracer) ([]uint32, error) {
	res, err := f.tokenList.FindPattern(sc.Context, tk, tids, tr)
	return res, err
}

func (f *Active) GetLIDsFromTIDs(tids []uint32, inv *inverser, _ lids.Counter, minLID, maxLID uint32, tr *tracer.Tracer, order seq.DocsOrder) []node.Node {
	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		m := tr.Start("provide")
		tlids := f.tokenList.Provide(tid)
		m.Stop()

		m = tr.Start("LIDs")
		unmapped := tlids.GetLIDs(f.mids, f.rids)
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
	return f.tokenList.GetValByTID(tid)
}

func (f *Active) Type() string {
	return frac.TypeActive
}

func (f *Active) Release(sealed frac.Fraction) {
	f.UseMu.Lock()
	defer f.UseMu.Unlock()

	f.sealed = sealed

	f.tokenList.Stop()

	f.rids = nil
	f.mids = nil
	f.tokenList = nil
	f.docsPositions = nil
}

func (f *Active) UpdateDiskStats(docsLen, metaLen uint64) uint64 {
	f.infoMu.Lock()
	defer f.infoMu.Unlock()

	pos := f.Base.Info.DocsOnDisk
	f.Base.Info.DocsOnDisk += docsLen
	f.Base.Info.MetaOnDisk += metaLen

	return pos
}

func (f *Active) close(closeDocs bool, hint string) {
	f.appender.Stop()
	if closeDocs {
		err := f.docsFile.Close()
		if err != nil {
			logger.Error("can't close docs file",
				zap.String("frac", f.BaseFileName),
				zap.String("type", "active"),
				zap.String("hint", hint),
				zap.Error(err))
		}
	}

	err := f.metaFile.Close()
	if err != nil {
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
	f.mids.mu.Lock()
	f.rids.mu.Lock()
	defer f.rids.mu.Unlock()
	defer f.mids.mu.Unlock()

	for _, id := range ids {
		lidsList = append(lidsList, f.mids.append(uint64(id.MID)))
		f.rids.append(uint64(id.RID))
	}

	return lidsList
}

func (f *Active) AppendID(id seq.ID) uint32 {
	return f.AppendIDs([]seq.ID{id})[0]
}

func (f *Active) UpdateStats(minMID, maxMID seq.MID, docCount uint32, sizeCount uint64) {
	f.infoMu.Lock()
	defer f.infoMu.Unlock()

	if f.Base.Info.From > minMID {
		f.Base.Info.From = minMID
	}
	if f.Base.Info.To < maxMID {
		f.Base.Info.To = maxMID
	}
	f.Base.Info.DocsTotal += docCount
	f.Base.Info.DocsRaw += sizeCount
}

func (f *Active) BuildInfoDistribution(ids []seq.ID) {
	info := f.Info()
	info.BuildDistribution(ids)

	f.infoMu.Lock()
	f.Base.Info = info
	f.infoMu.Unlock()
}

func (f *Active) Suicide() {
	f.UseMu.Lock()
	defer f.UseMu.Unlock()

	f.Suicided = true

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
	return frac.InfoToString(f.Info(), "active")
}

func (f *Active) DataProvider(ctx context.Context) (frac.DataProvider, func(), bool) {
	f.UseMu.RLock()
	if f.sealed != nil {
		defer f.UseMu.RUnlock()
		dp, releaseSealed, ok := f.sealed.DataProvider(ctx)
		metric.CountersTotal.WithLabelValues("use_sealed_from_active").Inc()
		return dp, releaseSealed, ok
	}

	if f.mids.Len() == 0 {
		f.UseMu.RUnlock()
		return nil, nil, false
	}

	dp := ActiveDataProvider{
		Active: f,
		sc:     frac.NewSearchCell(ctx),
		tracer: tracer.New(),
	}

	return &dp, func() {
		if dp.inverser != nil {
			dp.inverser.Release()
		}
		f.UseMu.RUnlock()
	}, true
}
