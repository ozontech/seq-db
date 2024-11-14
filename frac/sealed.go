package frac

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/tracer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

const seqDBMagic = "SEQM"

type SealedIDsProvider struct {
	ids         *SealedIDs
	midCache    *UnpackCache
	ridCache    *UnpackCache
	searchSB    *SearchCell
	fracVersion BinaryDataVersion
}

func (p *SealedIDsProvider) GetMID(lid seq.LID) seq.MID {
	p.ids.GetMIDsBlock(p.searchSB, seq.LID(lid), p.midCache)
	return seq.MID(p.midCache.GetValByLID(uint64(lid)))
}

func (p *SealedIDsProvider) GetRID(lid seq.LID) seq.RID {
	p.ids.GetRIDsBlock(p.searchSB, seq.LID(lid), p.ridCache, p.fracVersion)
	return seq.RID(p.ridCache.GetValByLID(uint64(lid)))
}

func (p *SealedIDsProvider) Len() int {
	return int(p.ids.IDsTotal)
}

func (p *SealedIDsProvider) LessOrEqual(lid seq.LID, id seq.ID) bool {
	if lid >= seq.LID(p.ids.IDsTotal) {
		// out of right border
		return true
	}

	blockIndex := p.ids.getIDBlockIndexByLID(lid)
	if !seq.LessOrEqual(p.ids.MinBlockIDs[blockIndex], id) {
		// the LID's block min ID is greater than the given ID, so any ID of that block is also greater
		return false
	}

	if blockIndex > 0 && seq.LessOrEqual(p.ids.MinBlockIDs[blockIndex-1], id) {
		// the min ID of the previous block is also less than or equal to the given ID,
		// so any ID of this block is definitely less than or equal to the given ID.
		return true
	}

	checkedMID := p.GetMID(lid)
	if checkedMID == id.MID {
		if id.RID == math.MaxUint64 {
			// this is a real use case for LessOrEqual
			// in this case the <= condition always becomes true,
			// so we don't need to load the RID from the disk
			return true
		}
		return p.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

type SealedDataProvider struct {
	*Sealed
	sc          *SearchCell
	tracer      *tracer.Tracer
	fracVersion BinaryDataVersion
}

func (dp *SealedDataProvider) Tracer() *tracer.Tracer {
	return dp.tracer
}

func (dp *SealedDataProvider) IDsProvider(midCache, ridCache *UnpackCache) IDsProvider {
	return &SealedIDsProvider{
		ids:         dp.ids,
		midCache:    midCache,
		ridCache:    ridCache,
		searchSB:    dp.sc,
		fracVersion: dp.fracVersion,
	}
}

func (dp *SealedDataProvider) GetTIDsByTokenExpr(t parser.Token, tids []uint32) ([]uint32, error) {
	return dp.Sealed.GetTIDsByTokenExpr(dp.sc, t, tids, dp.tracer)
}

func (dp *SealedDataProvider) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	return dp.Sealed.GetLIDsFromTIDs(dp.sc, tids, stats, minLID, maxLID, dp.tracer, order)
}

func (dp *SealedDataProvider) Fetch(id seq.ID, docsBuf []byte, midCache, ridCache *UnpackCache) ([]byte, []byte, error) {
	defer dp.tracer.UpdateMetric(metric.FetchSealedStagesSeconds)

	midCache.Start()
	ridCache.Start()

	defer midCache.Finish()
	defer ridCache.Finish()

	// bin search of LID by ID
	m := dp.tracer.Start("get_lid_by_mid")
	ids := dp.IDsProvider(midCache, ridCache)
	f := func(lid int) bool { return ids.LessOrEqual(seq.LID(lid), id) }
	lid := seq.LID(util.BinSearchInRange(1, ids.Len()-1, f))
	if id.MID != ids.GetMID(lid) || id.RID != ids.GetRID(lid) {
		m.Stop()
		return nil, docsBuf, nil
	}
	m.Stop()

	m = dp.tracer.Start("get_doc_params_by_lid")
	docPos := dp.ids.GetDocPosByLID(lid)
	m.Stop()

	m = dp.tracer.Start("get_doc_pos")
	blockPos, docOffset := dp.extractPosition(docPos)
	m.Stop()

	m = dp.tracer.Start("read_doc")
	dp.lastFetchTime.Store(time.Now().UnixNano())
	doc, outBuf, err := dp.readDoc(blockPos, 0, docOffset, docsBuf)
	m.Stop()

	return doc, outBuf, err
}

type Sealed struct {
	frac

	blocksReader *disk.BlocksReader

	tokenTable token.Table
	lidsTable  *lids.Table
	ids        *SealedIDs

	lastFetchTime atomic.Int64

	isLoaded bool
	loadMu   *sync.RWMutex

	cache *SealedIndexCache

	// shit for testing
	PartialSuicideMode PSD
}

type PSD int // emulates hard shutdown on different stages of fraction deletion, used for tests

const (
	Off PSD = iota
	HalfRename
	HalfRemove
)

func NewSealed(baseFile string, reader *disk.Reader, sealedIndexCache *SealedIndexCache, docBlockCache *cache.Cache[[]byte], fracInfoCache *Info) *Sealed {
	indexFileName := baseFile + consts.IndexFileSuffix

	r := disk.NewBlocksReader(sealedIndexCache.Registry, indexFileName, metric.StoreBytesRead)

	f := &Sealed{
		ids:          NewSealedIDs(reader, r, sealedIndexCache),
		blocksReader: r,
		loadMu:       &sync.RWMutex{},
		cache:        sealedIndexCache,
		frac: frac{
			docBlockCache: docBlockCache,
			reader:        reader,
			DocBlocks:     NewIDs(),
			info:          fracInfoCache,
			BaseFileName:  baseFile,
		},
		PartialSuicideMode: Off,
	}

	// fast path if fraction-info cache exists AND it has valid index size
	if fracInfoCache != nil && fracInfoCache.IndexOnDisk > 0 {
		return f
	}

	f.frac.info = &Info{Path: baseFile}
	f.loadHeader()

	return f
}

func NewSealedFromActive(active *Active, reader *disk.Reader, sealedIndexCache *SealedIndexCache) *Sealed {
	indexFileName := active.BaseFileName + consts.IndexFileSuffix
	blocksReader := disk.NewBlocksReader(sealedIndexCache.Registry, indexFileName, metric.StoreBytesRead)

	infoCopy := *active.info
	f := &Sealed{
		// the data of these three fields will actually be read from disk again in the future on the first
		// attempt to search in fraction (see method Sealed.loadAndRLock())
		// TODO: we need to either remove this data preparation in active fraction sealing or avoid re-reading the data from disk
		tokenTable:   active.tokenTable,
		lidsTable:    active.lidsTable,
		ids:          active.sealedIDs,
		blocksReader: blocksReader,
		loadMu:       &sync.RWMutex{},
		cache:        sealedIndexCache,
		frac: frac{
			docBlockCache: active.frac.docBlockCache,
			reader:        reader,
			docsFile:      active.docsFile,
			info:          &infoCopy,
			DocBlocks:     active.DocBlocks,
			BaseFileName:  active.BaseFileName,
		},
	}

	f.ids.Reader = reader
	f.ids.BlocksReader = blocksReader
	f.ids.cache = sealedIndexCache

	docsCountK := float64(f.info.DocsTotal) / 1000
	logger.Info("sealed fraction created from active",
		zap.String("frac", f.info.Name()),
		util.ZapMsTsAsESTimeStr("creation_time", f.info.CreationTime),
		zap.String("from", f.info.From.String()),
		zap.String("to", f.info.To.String()),
		util.ZapFloat64WithPrec("docs_k", docsCountK, 1),
	)

	f.info.MetaOnDisk = 0

	return f
}

func (f *Sealed) loadHeader() {
	readTask := f.reader.ReadIndexBlock(f.blocksReader, 0, nil)

	if readTask.Err != nil {
		logger.Panic("todo")
	}
	result := readTask.Buf
	if len(result) < 4 || string(result[:4]) != seqDBMagic {
		logger.Fatal("seq-db index file header corrupted",
			zap.String("file", f.blocksReader.GetFileName()),
		)
	}

	result = result[4:]
	func() {
		f.statsMu.Lock()
		defer f.statsMu.Unlock()

		f.info.Load(result)
		f.info.MetaOnDisk = 0
		stat, err := f.blocksReader.GetFileStat()
		if err != nil {
			logger.Fatal("can't stat index file",
				zap.String("file", f.blocksReader.GetFileName()),
				zap.Error(err),
			)
			panic("_")
		}
		f.info.IndexOnDisk = uint64(stat.Size())
	}()
}

func (f *Sealed) Type() string {
	return TypeSealed
}

func (f *Sealed) loadAndRLock() {
	// todo: replace this shitty algorithm
	for {
		f.loadMu.RLock()
		if f.isLoaded {
			return
		}
		f.loadMu.RUnlock()

		func() {
			f.loadMu.Lock()
			defer f.loadMu.Unlock()
			if !f.isLoaded {
				(&Loader{}).Load(f)
				f.isLoaded = true
			}
		}()
	}
}

func (f *Sealed) loadRUnlock() {
	f.loadMu.RUnlock()
}

func (f *Sealed) GetTokenBlockLoader(stats *SearchCell) *token.BlockLoader {
	return token.NewBlockLoader(f.BaseFileName, f.reader, f.blocksReader, f.cache.Tokens, stats)
}

func (f *Sealed) GetValByTID(tid uint32) []byte {
	if entry := f.tokenTable.GetEntryByTID(tid); entry != nil {
		loader := f.GetTokenBlockLoader(nil)
		return loader.Load(entry).GetValByTID(tid)
	}
	return nil
}

func (f *Sealed) GetTIDsByTokenExpr(searchSB *SearchCell, t parser.Token, tids []uint32, _ *tracer.Tracer) ([]uint32, error) {
	field := parser.GetField(t)
	searchStr := parser.GetHint(t)

	entries := f.tokenTable.SelectEntries(field, searchStr)
	if len(entries) == 0 {
		return tids, nil
	}

	loader := f.GetTokenBlockLoader(searchSB)
	fetcher := token.NewFetcher(loader, entries)
	searcher := pattern.NewSearcher(t, fetcher, fetcher.GetTokensCount())

	begin := searcher.Begin()
	end := searcher.End()
	if begin > end {
		return tids, nil
	}

	blockIndex := fetcher.GetBlockIndex(begin)
	lastTID := fetcher.GetTIDFromIndex(end)

	entry := entries[blockIndex]
	tokensBlock := loader.Load(entry)
	entryLastTID := entry.GetLastTID()

	for tid := fetcher.GetTIDFromIndex(begin); tid <= lastTID; tid++ {
		if tid > entryLastTID {
			if searchSB.Exit.Load() {
				return nil, consts.ErrUnexpectedInterruption
			}
			if searchSB.IsCancelled() {
				err := fmt.Errorf("search cancelled when matching tokens: reason=%s field=%s, query=%s", searchSB.Context.Err(), field, searchStr)
				searchSB.Cancel(err)
				return nil, err
			}
			blockIndex++
			entry = entries[blockIndex]
			tokensBlock = loader.Load(entry)
			entryLastTID = entry.GetLastTID()
		}

		val := tokensBlock.GetValByTID(tid)
		if searcher.Check(val) {
			tids = append(tids, tid)
		}
	}

	return tids, nil
}

func (f *Sealed) GetLIDsFromTIDs(sc *SearchCell, tids []uint32, counter lids.Counter, minLID, maxLID uint32, tr *tracer.Tracer, order seq.DocsOrder) []node.Node {
	m := tr.Start("GetOpTIDLIDs")
	defer m.Stop()

	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	loader := lids.NewLoader(f.reader, f.blocksReader, f.cache.LIDs, sc)

	if order.IsReverse() {
		getBlockIndex = func(tid uint32) uint32 { return f.lidsTable.GetLastBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorAsc)(lids.NewLIDsCursor(f.lidsTable, loader, startIndex, tid, counter, minLID, maxLID))
		}
	} else {
		getBlockIndex = func(tid uint32) uint32 { return f.lidsTable.GetFirstBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorDesc)(lids.NewLIDsCursor(f.lidsTable, loader, startIndex, tid, counter, minLID, maxLID))
		}
	}

	t := time.Now()
	startIndexes := make([]uint32, len(tids))
	for i, tid := range tids {
		startIndexes[i] = getBlockIndex(tid)
	}
	sc.AddLIDBlocksSearchTimeNS(time.Since(t))

	nodes := make([]node.Node, len(tids))
	for i, tid := range tids {
		nodes[i] = getLIDsIterator(startIndexes[i], tid)
	}

	return nodes
}

func (f *Sealed) extractPosition(docPos DocPos) (uint64, uint64) {
	docBlockIndex, docOffset := docPos.Unpack()
	blockPoses := f.DocBlocks.GetVals()
	if docBlockIndex >= uint32(len(blockPoses)) {
		logger.Panic("can't get block pos",
			zap.Uint32("block_index", docBlockIndex),
			zap.Int("len", len(blockPoses)),
		)
	}
	return blockPoses[docBlockIndex], docOffset
}

func (f *Sealed) Suicide() {
	f.useLock.Lock()
	defer f.useLock.Unlock()

	f.suicided = true

	f.close("suicide")

	f.cache.Release()
	f.docBlockCache.Release()

	// make some atomic magic, to be more stable on removing fractions
	oldPath := f.BaseFileName + consts.DocsFileSuffix
	newPath := f.BaseFileName + consts.DocsDelFileSuffix
	err := os.Rename(oldPath, newPath)
	if err != nil {
		logger.Error("can't rename docs file",
			zap.String("old_path", oldPath),
			zap.String("new_path", newPath),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRename {
		return
	}

	oldPath = f.BaseFileName + consts.IndexFileSuffix
	newPath = f.BaseFileName + consts.IndexDelFileSuffix
	err = os.Rename(oldPath, newPath)
	if err != nil {
		logger.Error("can't rename index file",
			zap.String("old_path", oldPath),
			zap.String("new_path", newPath),
			zap.Error(err),
		)
	}

	rmPath := f.BaseFileName + consts.DocsDelFileSuffix
	err = os.Remove(rmPath)
	if err != nil {
		logger.Error("can't remove docs file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}

	if f.PartialSuicideMode == HalfRemove {
		return
	}

	rmPath = f.BaseFileName + consts.IndexDelFileSuffix
	err = os.Remove(rmPath)
	if err != nil {
		logger.Error("can't remove index file",
			zap.String("file", rmPath),
			zap.Error(err),
		)
	}
}

func (f *Sealed) close(hint string) {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {
		return
	}

	// docs file may not be opened since it's loaded lazily
	if f.docsFile != nil {
		err := f.docsFile.Close()
		if err != nil {
			logger.Error("can't close docs file",
				f.closeLogArgs("sealed", hint, err)...,
			)
		}
	}

	err := f.blocksReader.Close()
	if err != nil {
		logger.Error("can't close index file",
			f.closeLogArgs("sealed", hint, err)...,
		)
	}
}

func (f *Sealed) FullSize() uint64 {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()
	return f.info.DocsOnDisk + f.info.IndexOnDisk
}

func (f *Sealed) String() string {
	return f.toString("sealed")
}

func (f *Sealed) DataProvider(ctx context.Context) (DataProvider, func(), bool) {
	f.useLock.RLock()

	defer func() {
		if panicData := recover(); panicData != nil {
			f.useLock.RUnlock()
			panic(panicData)
		}
	}()

	if f.suicided {
		metric.CountersTotal.WithLabelValues("request_suicided").Inc()
		f.useLock.RUnlock()
		return nil, nil, false
	}

	f.loadAndRLock()

	dp := SealedDataProvider{
		Sealed:      f,
		sc:          NewSearchCell(ctx),
		tracer:      tracer.New(),
		fracVersion: f.info.BinaryDataVer,
	}

	return &dp, func() {
		f.loadRUnlock()
		f.useLock.RUnlock()
	}, true
}
