package sealed

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/tracer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SealedIDsProvider struct {
	ids         *IDs
	midCache    *UnpackCache
	ridCache    *UnpackCache
	searchSB    *frac.SearchCell
	fracVersion frac.BinaryDataVersion
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
	sc               *frac.SearchCell
	tracer           *tracer.Tracer
	fracVersion      frac.BinaryDataVersion
	midCache         *UnpackCache
	ridCache         *UnpackCache
	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader
}

func (dp *SealedDataProvider) Tracer() *tracer.Tracer {
	return dp.tracer
}

func (dp *SealedDataProvider) IDsProvider() frac.IDsProvider {
	return &SealedIDsProvider{
		ids:         dp.ids,
		midCache:    dp.midCache,
		ridCache:    dp.ridCache,
		searchSB:    dp.sc,
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
			if dp.sc.Exit.Load() {
				return nil, consts.ErrUnexpectedInterruption
			}
			if dp.sc.IsCancelled() {
				err := fmt.Errorf("search cancelled when matching tokens: reason=%s field=%s, query=%s", dp.sc.Context.Err(), field, searchStr)
				dp.sc.Cancel(err)
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
	return dp.Sealed.GetLIDsFromTIDs(dp.sc, tids, stats, minLID, maxLID, dp.tracer, order)
}

// findLIDs returns a slice of LIDs. If seq.ID is not found, LID has the value 0 at the corresponding position
func findLIDs(p frac.IDsProvider, ids []seq.ID) []seq.LID {
	res := make([]seq.LID, len(ids))

	// left and right it is search range
	left := 1            // first
	right := p.Len() - 1 // last

	for i, id := range ids {

		if i == 0 || !seq.Less(id, ids[i-1]) {
			// reset search range (it is not DESC sorted IDs)
			left = 1
		}

		lid := seq.LID(util.BinSearchInRange(left, right, func(lid int) bool {
			return p.LessOrEqual(seq.LID(lid), id)
		}))

		if id.MID == p.GetMID(lid) && id.RID == p.GetRID(lid) {
			res[i] = lid
		}

		// try to refine the search range, but this optimization works for DESC sorted IDs only
		left = int(lid)
	}

	return res
}

func (dp *SealedDataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	defer dp.tracer.UpdateMetric(metric.FetchSealedStagesSeconds)

	m := dp.tracer.Start("get_lid_by_mid")
	l := findLIDs(dp.IDsProvider(), ids)
	m.Stop()

	m = dp.tracer.Start("get_doc_params_by_lid")
	docsPos := dp.ids.GetDocPosByLIDs(l)
	m.Stop()

	m = dp.tracer.Start("get_doc_pos")
	blocks, offsets, index := frac.GroupDocsOffsets(docsPos)
	m.Stop()

	m = dp.tracer.Start("read_doc")
	res := make([][]byte, len(ids))
	for i, docOffsets := range offsets {
		docs, err := dp.Base.DocsReader.Read(dp.blocksOffsets[blocks[i]], docOffsets)
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

type Sealed struct {
	frac.Base

	reader       *disk.Reader
	blocksReader *disk.RegistryReader

	lidsTable *lids.Table
	ids       *IDs

	blocksOffsets []uint64

	isLoaded bool
	loadMu   *sync.RWMutex

	docsCache  *cache.Cache[[]byte]
	indexCache *IndexCache

	docsFile  *os.File
	indexFile *os.File

	// shit for testing
	PartialSuicideMode PSD
}

type PSD int // emulates hard shutdown on different stages of fraction deletion, used for tests

const (
	Off PSD = iota
	HalfRename
	HalfRemove
)

func NewSealed(baseFile string, reader *disk.Reader, indexCache *IndexCache, docsCache *cache.Cache[[]byte], infoCached frac.Info) *Sealed {
	f := &Sealed{
		reader:     reader,
		loadMu:     &sync.RWMutex{},
		docsCache:  docsCache,
		indexCache: indexCache,
		Base: frac.Base{
			Info:         infoCached,
			BaseFileName: baseFile,
		},
		PartialSuicideMode: Off,
	}

	if f.Base.Info.IndexOnDisk > 0 { // fast path if fraction-info cache exists AND it has valid index size
		return f
	}

	f.Base.Info = f.loadHeader()

	return f
}

func (f *Sealed) Info() frac.Info {
	return f.Base.Info
}

func (f *Sealed) Contains(id seq.MID) bool {
	return f.IsIntersecting(id, id)
}

func (f *Sealed) IsIntersecting(from, to seq.MID) bool {
	return f.Base.Info.IsIntersecting(from, to)
}

func (f *Sealed) readHeader() frac.Info {
	f.initIndexReader()
	block, _, err := f.reader.ReadIndexBlock(f.blocksReader, 0, nil)
	if err != nil {
		logger.Panic("todo")
	}
	if len(block) < 4 || string(block[:4]) != seqDBMagic {
		logger.Fatal("seq-db index file header corrupted", zap.String("file", f.indexFile.Name()))
	}
	info := frac.Info{}
	info.Load(block[4:])
	return info
}

func (f *Sealed) loadHeader() frac.Info {
	info := f.readHeader()
	info.Path = f.BaseFileName
	info.MetaOnDisk = 0
	info.IndexOnDisk = f.getIndexSize()
	return info
}

func (f *Sealed) getIndexSize() uint64 {
	stat, err := f.indexFile.Stat()
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", f.indexFile.Name()), zap.Error(err))
	}
	return uint64(stat.Size())
}

func (f *Sealed) Type() string {
	return frac.TypeSealed
}

func (f *Sealed) loadAndRLock() {
	f.loadMu.RLock()
	if !f.isLoaded {
		f.loadMu.RUnlock()
		f.load()
		f.loadMu.RLock()
	}
}

func (f *Sealed) load() {
	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	if !f.isLoaded {
		f.initDocsReader()
		f.initIndexReader()
		(&Loader{}).Load(f)
		f.isLoaded = true
	}
}

func (f *Sealed) initDocsReader() {
	if f.docsFile != nil {
		return
	}
	var err error
	if f.docsFile, err = os.Open(f.BaseFileName + consts.DocsFileSuffix); err != nil {
		logger.Fatal("can't open docs file", zap.String("file", f.BaseFileName), zap.Error(err))
	}
	f.Base.DocsReader = frac.NewDocsReader(f.docsFile, f.reader, f.docsCache)
}

func (f *Sealed) initIndexReader() {
	if f.indexFile != nil {
		return
	}
	var err error
	if f.indexFile, err = os.Open(f.BaseFileName + consts.IndexFileSuffix); err != nil {
		logger.Fatal("can't open index file", zap.String("file", f.BaseFileName), zap.Error(err))
	}
	f.blocksReader = disk.NewRegistryReader(f.indexCache.Registry, f.indexFile, metric.StoreBytesRead)
	f.ids = NewSealedIDs(f.reader, f.blocksReader, f.indexCache)
}

func (f *Sealed) Preload(dr frac.DocsReader, lt *lids.Table, bo []uint64, ids *IDs) {
	f.initIndexReader()

	f.docsFile = dr.File
	f.Base.DocsReader = dr

	f.lidsTable = lt
	f.blocksOffsets = bo
	f.ids.LoadFrom(ids)

	f.isLoaded = true
}

func (f *Sealed) GetLIDsFromTIDs(sc *frac.SearchCell, tids []uint32, counter lids.Counter, minLID, maxLID uint32, tr *tracer.Tracer, order seq.DocsOrder) []node.Node {
	m := tr.Start("GetOpTIDLIDs")
	defer m.Stop()

	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	loader := lids.NewLoader(f.reader, f.blocksReader, f.indexCache.LIDs, sc)

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

func (f *Sealed) Suicide() {
	f.UseMu.Lock()
	defer f.UseMu.Unlock()

	f.Suicided = true

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
	if f.docsFile != nil { // docs file may not be opened since it's loaded lazily
		if err := f.docsFile.Close(); err != nil {
			logger.Error("can't close docs file",
				zap.String("frac", f.BaseFileName),
				zap.String("type", "sealed"),
				zap.String("hint", hint),
				zap.Error(err))
		}
	}

	if f.indexFile != nil { // index file may not be opened since it's loaded lazily
		if err := f.indexFile.Close(); err != nil {
			logger.Error("can't close index file",
				zap.String("frac", f.BaseFileName),
				zap.String("type", "sealed"),
				zap.String("hint", hint),
				zap.Error(err))
		}
	}
}

func (f *Sealed) FullSize() uint64 {
	return f.Base.Info.DocsOnDisk + f.Base.Info.IndexOnDisk
}

func (f *Sealed) String() string {
	return frac.InfoToString(f.Info(), "sealed")
}

func (f *Sealed) DataProvider(ctx context.Context) (frac.DataProvider, func(), bool) {
	f.UseMu.RLock()

	defer func() {
		if panicData := recover(); panicData != nil {
			f.UseMu.RUnlock()
			panic(panicData)
		}
	}()

	if f.Suicided {
		metric.CountersTotal.WithLabelValues("request_suicided").Inc()
		f.UseMu.RUnlock()
		return nil, nil, false
	}

	f.loadAndRLock()

	sc := frac.NewSearchCell(ctx)
	dp := SealedDataProvider{
		Sealed:           f,
		sc:               sc,
		tracer:           tracer.New(),
		fracVersion:      f.Base.Info.BinaryDataVer,
		midCache:         NewUnpackCache(),
		ridCache:         NewUnpackCache(),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.reader, f.blocksReader, f.indexCache.Tokens, sc),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.reader, f.blocksReader, f.indexCache.TokenTable),
	}

	return &dp, func() {
		dp.midCache.Release()
		dp.ridCache.Release()

		f.loadMu.RUnlock()
		f.UseMu.RUnlock()
	}, true
}
