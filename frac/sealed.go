package frac

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

const seqDBMagic = "SEQM"

type SealedIDsIndex struct {
	loader      *IDsLoader
	midCache    *UnpackCache
	ridCache    *UnpackCache
	fracVersion BinaryDataVersion
}

func (p *SealedIDsIndex) GetMID(lid seq.LID) seq.MID {
	p.loader.GetMIDsBlock(seq.LID(lid), p.midCache)
	return seq.MID(p.midCache.GetValByLID(uint64(lid)))
}

func (p *SealedIDsIndex) GetRID(lid seq.LID) seq.RID {
	p.loader.GetRIDsBlock(seq.LID(lid), p.ridCache, p.fracVersion)
	return seq.RID(p.ridCache.GetValByLID(uint64(lid)))
}

func (p *SealedIDsIndex) Len() int {
	return int(p.loader.table.IDsTotal)
}

func (p *SealedIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	if lid >= seq.LID(p.loader.table.IDsTotal) {
		// out of right border
		return true
	}

	blockIndex := p.loader.getIDBlockIndexByLID(lid)
	if !seq.LessOrEqual(p.loader.table.MinBlockIDs[blockIndex], id) {
		// the LID's block min ID is greater than the given ID, so any ID of that block is also greater
		return false
	}

	if blockIndex > 0 && seq.LessOrEqual(p.loader.table.MinBlockIDs[blockIndex-1], id) {
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
	ctx              context.Context
	sw               *stopwatch.Stopwatch
	fracVersion      BinaryDataVersion
	midCache         *UnpackCache
	ridCache         *UnpackCache
	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader
}

func (dp *SealedDataProvider) Stopwatch() *stopwatch.Stopwatch {
	return dp.sw
}

func (dp *SealedDataProvider) IDsIndex() IDsIndex {
	return &SealedIDsIndex{
		loader:      NewIDsLoader(dp.indexReader, dp.indexCache, dp.idsTable),
		midCache:    dp.midCache,
		ridCache:    dp.ridCache,
		fracVersion: dp.fracVersion,
	}
}

func (dp *SealedDataProvider) GetValByTID(tid uint32) []byte {
	tokenTable := dp.tokenTableLoader.Load()
	if entry := tokenTable.GetEntryByTID(tid); entry != nil {
		return dp.tokenBlockLoader.Load(entry).GetValByTID(tid)
	}
	return nil
}

func (dp *SealedDataProvider) GetTIDsByTokenExpr(t parser.Token, tids []uint32) ([]uint32, error) {
	field := parser.GetField(t)
	searchStr := parser.GetHint(t)

	tokenTable := dp.tokenTableLoader.Load()
	entries := tokenTable.SelectEntries(field, searchStr)
	if len(entries) == 0 {
		return tids, nil
	}

	fetcher := token.NewFetcher(dp.tokenBlockLoader, entries)
	searcher := pattern.NewSearcher(t, fetcher, fetcher.GetTokensCount())

	begin := searcher.Begin()
	end := searcher.End()
	if begin > end {
		return tids, nil
	}

	blockIndex := fetcher.GetBlockIndex(begin)
	lastTID := fetcher.GetTIDFromIndex(end)

	entry := entries[blockIndex]
	tokensBlock := dp.tokenBlockLoader.Load(entry)
	entryLastTID := entry.GetLastTID()

	for tid := fetcher.GetTIDFromIndex(begin); tid <= lastTID; tid++ {
		if tid > entryLastTID {
			if util.IsCancelled(dp.ctx) {
				err := fmt.Errorf("search cancelled when matching tokens: reason=%s field=%s, query=%s", dp.ctx.Err(), field, searchStr)
				return nil, err
			}
			blockIndex++
			entry = entries[blockIndex]
			tokensBlock = dp.tokenBlockLoader.Load(entry)
			entryLastTID = entry.GetLastTID()
		}

		val := tokensBlock.GetValByTID(tid)
		if searcher.Check(val) {
			tids = append(tids, tid)
		}
	}

	return tids, nil
}

func (dp *SealedDataProvider) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	return dp.Sealed.GetLIDsFromTIDs(tids, stats, minLID, maxLID, dp.sw, order)
}

func (dp *SealedDataProvider) DocsIndex() DocsIndex {
	return &SealedDocsIndex{
		idsIndex: dp.IDsIndex(),
		idsLoader: NewIDsLoader(
			dp.Sealed.indexReader,
			dp.Sealed.indexCache,
			dp.Sealed.idsTable,
		),
		docsReader:    dp.Sealed.docsReader,
		blocksOffsets: dp.Sealed.BlocksOffsets,
	}
}

type SealedDocsIndex struct {
	idsIndex      IDsIndex
	idsLoader     *IDsLoader
	docsReader    *disk.DocsReader
	blocksOffsets []uint64
}

func (di *SealedDocsIndex) GetBlocksOffsets(num uint32) uint64 {
	return di.blocksOffsets[num]
}

func (di *SealedDocsIndex) GetDocPos(ids []seq.ID) []DocPos {
	return di.getDocPosByLIDs(di.findLIDs(ids))
}

func (di *SealedDocsIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return di.docsReader.ReadDocs(blockOffset, docOffsets)
}

// findLIDs returns a slice of LIDs. If seq.ID is not found, LID has the value 0 at the corresponding position
func (di *SealedDocsIndex) findLIDs(ids []seq.ID) []seq.LID {
	res := make([]seq.LID, len(ids))

	// left and right it is search range
	left := 1                      // first
	right := di.idsIndex.Len() - 1 // last

	for i, id := range ids {

		if i == 0 || !seq.Less(id, ids[i-1]) {
			// reset search range (it is not DESC sorted IDs)
			left = 1
		}

		lid := seq.LID(util.BinSearchInRange(left, right, func(lid int) bool {
			return di.idsIndex.LessOrEqual(seq.LID(lid), id)
		}))

		if id.MID == di.idsIndex.GetMID(lid) && id.RID == di.idsIndex.GetRID(lid) {
			res[i] = lid
		}

		// try to refine the search range, but this optimization works for DESC sorted IDs only
		left = int(lid)
	}

	return res
}

// GetDocPosByLIDs returns a slice of DocPos for the corresponding LIDs.
// Passing sorted LIDs (asc or desc) will improve the performance of this method.
// For LID with zero value will return DocPos with `DocPosNotFound` value
func (di *SealedDocsIndex) getDocPosByLIDs(localIDs []seq.LID) []DocPos {
	var (
		prevIndex int64
		positions []uint64
		startLID  seq.LID
	)

	res := make([]DocPos, len(localIDs))
	for i, lid := range localIDs {
		if lid == 0 {
			res[i] = DocPosNotFound
			continue
		}

		index := di.idsLoader.getIDBlockIndexByLID(lid)
		if positions == nil || prevIndex != index {
			positions = di.idsLoader.GetParamsBlock(uint32(index))
			startLID = seq.LID(index * consts.IDsPerBlock)
		}

		res[i] = DocPos(positions[lid-startLID])
	}

	return res
}

type Sealed struct {
	frac

	docsFile   *os.File
	docsCache  *cache.Cache[[]byte]
	docsReader *disk.DocsReader

	indexFile   *os.File
	indexCache  *IndexCache
	indexReader *disk.IndexReader

	idsTable      IDsTable
	lidsTable     *lids.Table
	BlocksOffsets []uint64

	loadMu   *sync.RWMutex
	isLoaded bool

	readLimiter *disk.ReadLimiter

	// shit for testing
	PartialSuicideMode PSD
}

type PSD int // emulates hard shutdown on different stages of fraction deletion, used for tests

const (
	Off PSD = iota
	HalfRename
	HalfRemove
)

func NewSealed(baseFile string, readLimiter *disk.ReadLimiter, indexCache *IndexCache, docsCache *cache.Cache[[]byte], fracInfoCache *Info) *Sealed {
	f := &Sealed{
		loadMu: &sync.RWMutex{},

		readLimiter: readLimiter,
		docsCache:   docsCache,
		indexCache:  indexCache,

		frac: frac{
			info:         fracInfoCache,
			BaseFileName: baseFile,
		},
		PartialSuicideMode: Off,
	}

	// fast path if fraction-info cache exists AND it has valid index size
	if fracInfoCache != nil && fracInfoCache.IndexOnDisk > 0 {
		return f
	}

	f.openIndex()
	f.info = f.loadHeader()

	return f
}

func (f *Sealed) openIndex() {
	if f.indexReader == nil {
		var err error
		name := f.BaseFileName + consts.IndexFileSuffix
		f.indexFile, err = os.Open(name)
		if err != nil {
			logger.Fatal("can't open index file", zap.String("file", name), zap.Error(err))
		}
		f.indexReader = disk.NewIndexReader(f.readLimiter, f.indexFile, f.indexCache.Registry)
	}
}

func (f *Sealed) openDocs() {
	if f.docsReader == nil {
		var err error
		name := f.BaseFileName + consts.DocsFileSuffix
		f.docsFile, err = os.Open(name)
		if err != nil {
			logger.Fatal("can't open docs file", zap.String("file", name), zap.Error(err))
		}
		f.docsReader = disk.NewDocsReader(f.readLimiter, f.docsFile, f.docsCache)
	}
}

func NewSealedFromActive(active *Active, readLimiter *disk.ReadLimiter, indexFile *os.File, indexCache *IndexCache) *Sealed {
	infoCopy := *active.info
	f := &Sealed{
		idsTable:      active.idsTable,
		lidsTable:     active.lidsTable,
		BlocksOffsets: active.DocBlocks.GetVals(),

		docsFile:   active.docsFile,
		docsCache:  active.docsCache,
		docsReader: active.docsReader,

		indexFile:   indexFile,
		indexCache:  indexCache,
		indexReader: disk.NewIndexReader(readLimiter, indexFile, indexCache.Registry),

		loadMu:   &sync.RWMutex{},
		isLoaded: true,

		readLimiter: readLimiter,

		frac: frac{
			info:         &infoCopy,
			BaseFileName: active.BaseFileName,
		},
	}

	// put the token table built during sealing into the cache of the sealed faction
	indexCache.TokenTable.Get(token.CacheKeyTable, func() (token.Table, int) {
		return active.tokenTable, active.tokenTable.Size()
	})

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

func (f *Sealed) loadHeader() *Info {
	block, _, err := f.indexReader.ReadIndexBlock(0, nil)
	if err != nil {
		logger.Panic("todo")
	}
	if len(block) < 4 || string(block[:4]) != seqDBMagic {
		logger.Fatal("seq-db index file header corrupted", zap.String("file", f.indexFile.Name()))
	}
	info := &Info{}
	info.Load(block[4:])

	stat, err := f.indexFile.Stat()
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", f.indexFile.Name()), zap.Error(err))
	}

	info.MetaOnDisk = 0        // todo: make this correction on sealing
	info.Path = f.BaseFileName // todo: make this correction on sealing
	info.IndexOnDisk = uint64(stat.Size())

	return info
}

func (f *Sealed) Type() string {
	return TypeSealed
}

func (f *Sealed) load() {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {

		f.openDocs()
		f.openIndex()

		(&Loader{}).Load(f)
		f.isLoaded = true
	}
}

func (f *Sealed) GetLIDsFromTIDs(tids []uint32, counter lids.Counter, minLID, maxLID uint32, sw *stopwatch.Stopwatch, order seq.DocsOrder) []node.Node {
	m := sw.Start("GetOpTIDLIDs")
	defer m.Stop()

	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	loader := lids.NewLoader(f.indexReader, f.indexCache.LIDs)

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

	startIndexes := make([]uint32, len(tids))
	for i, tid := range tids {
		startIndexes[i] = getBlockIndex(tid)
	}

	nodes := make([]node.Node, len(tids))
	for i, tid := range tids {
		nodes[i] = getLIDsIterator(startIndexes[i], tid)
	}

	return nodes
}

func (f *Sealed) Suicide() {
	f.useLock.Lock()
	f.suicided = true
	f.useLock.Unlock()

	f.close("suicide")

	f.docsCache.Release()
	f.indexCache.Release()

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

	if f.docsFile != nil { // docs file may not be opened since it's loaded lazily
		if err := f.docsFile.Close(); err != nil {
			logger.Error("can't close docs file", f.closeLogArgs("sealed", hint, err)...)
		}
	}

	if err := f.indexFile.Close(); err != nil {
		logger.Error("can't close index file", f.closeLogArgs("sealed", hint, err)...)
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

	if f.suicided {
		metric.CountersTotal.WithLabelValues("fraction_suicided").Inc()
		f.useLock.RUnlock()
		return nil, nil, false
	}

	defer func() {
		if panicData := recover(); panicData != nil {
			f.useLock.RUnlock()
			panic(panicData)
		}
	}()

	f.load()

	dp := SealedDataProvider{
		Sealed:           f,
		ctx:              ctx,
		sw:               stopwatch.New(),
		fracVersion:      f.info.BinaryDataVer,
		midCache:         NewUnpackCache(),
		ridCache:         NewUnpackCache(),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.indexReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.indexReader, f.indexCache.TokenTable),
	}

	return &dp, func() {
		dp.midCache.Release()
		dp.ridCache.Release()
		f.useLock.RUnlock()
	}, true
}
