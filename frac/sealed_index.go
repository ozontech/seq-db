package frac

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

var (
	fetcherSealedStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "sealed_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

	sealedAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	sealedHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	sealedRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_sealed_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
)

type sealedDataProvider struct {
	ctx    context.Context
	info   *Info
	config *Config

	idsTable    *seqids.Table
	idsProvider *seqids.Provider

	lidsTable  *lids.Table
	lidsLoader *lids.Loader

	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader

	blocksOffsets []uint64
	docsReader    *storage.DocsReader
}

func (dp *sealedDataProvider) getIDsIndex() *sealedIDsIndex {
	return &sealedIDsIndex{
		fracName: dp.info.Name(),
		provider: dp.idsProvider,
		table:    dp.idsTable,
	}
}

func (dp *sealedDataProvider) getFetchIndex() *sealedFetchIndex {
	return &sealedFetchIndex{
		idsIndex:      dp.getIDsIndex(),
		docsReader:    dp.docsReader,
		blocksOffsets: dp.blocksOffsets,
	}
}

func (dp *sealedDataProvider) getTokenIndex() *sealedTokenIndex {
	return &sealedTokenIndex{
		ctx:              dp.ctx,
		lidsLoader:       dp.lidsLoader,
		lidsTable:        dp.lidsTable,
		tokenTableLoader: dp.tokenTableLoader,
		tokenBlockLoader: dp.tokenBlockLoader,
	}
}

func (dp *sealedDataProvider) getSearchIndex() *sealedSearchIndex {
	return &sealedSearchIndex{
		sealedIDsIndex:   dp.getIDsIndex(),
		sealedTokenIndex: dp.getTokenIndex(),
	}
}

func (dp *sealedDataProvider) release() {
	dp.idsProvider.Release()
}

func (dp *sealedDataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	res := make([][]byte, len(ids))
	if err := processor.IndexFetch(ids, sw, dp.getFetchIndex(), res); err != nil {
		return nil, err
	}
	sw.Export(fetcherSealedStagesSeconds)

	return res, nil
}

func (dp *sealedDataProvider) Search(params processor.SearchParams) (*seq.QPR, error) {
	aggLimits := processor.AggLimits(dp.config.Search.AggLimits)

	sw := stopwatch.New()
	defer sw.Export(getSealedSearchMetric(params))

	t := sw.Start("total")
	qpr, err := processor.IndexSearch(dp.ctx, params, dp.getSearchIndex(), aggLimits, sw)
	if err != nil {
		return nil, err
	}
	qpr.IDs.ApplyHint(dp.info.Name())
	t.Stop()

	return qpr, nil
}

func getSealedSearchMetric(params processor.SearchParams) *prometheus.HistogramVec {
	if params.HasAgg() {
		return sealedAggSearchSec
	}
	if params.HasHist() {
		return sealedHistSearchSec
	}
	return sealedRegSearchSec
}

type sealedIDsIndex struct {
	fracName string
	table    *seqids.Table
	provider *seqids.Provider
}

func (ii *sealedIDsIndex) GetMID(lid seq.LID) seq.MID {
	mid, err := ii.provider.MID(lid)
	if err != nil {
		logger.Panic("get mid error", zap.String("frac", ii.fracName), zap.Uint32("lid", uint32(lid)), zap.Error(err))
	}
	return mid
}

func (ii *sealedIDsIndex) GetRID(lid seq.LID) seq.RID {
	rid, err := ii.provider.RID(lid)
	if err != nil {
		logger.Panic("get rid error", zap.String("frac", ii.fracName), zap.Uint32("lid", uint32(lid)), zap.Error(err))
	}
	return rid
}

func (ii *sealedIDsIndex) docPos(lid seq.LID) seq.DocPos {
	pos, err := ii.provider.DocPos(lid)
	if err != nil {
		logger.Panic("get DocPos error", zap.String("frac", ii.fracName), zap.Uint32("lid", uint32(lid)), zap.Error(err))
	}
	return pos
}

func (ii *sealedIDsIndex) Len() int {
	return int(ii.table.IDsTotal)
}

func (ii *sealedIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	if lid >= seq.LID(ii.table.IDsTotal) {
		// out of right border
		return true
	}

	blockIndex := ii.table.GetIDBlockIndexByLID(uint32(lid))
	if !seq.LessOrEqual(ii.table.MinBlockIDs[blockIndex], id) {
		// the LID's block min ID is greater than the given ID, so any ID of that block is also greater
		return false
	}

	if blockIndex > 0 && seq.LessOrEqual(ii.table.MinBlockIDs[blockIndex-1], id) {
		// the min ID of the previous block is also less than or equal to the given ID,
		// so any ID of this block is definitely less than or equal to the given ID.
		return true
	}

	checkedMID := ii.GetMID(lid)
	if checkedMID == id.MID {
		if id.RID == math.MaxUint64 {
			// this is a real use case for LessOrEqual
			// in this case the <= condition always becomes true,
			// so we don't need to load the RID from the disk
			return true
		}
		return ii.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

type sealedTokenIndex struct {
	ctx              context.Context
	lidsTable        *lids.Table
	lidsLoader       *lids.Loader
	tokenTableLoader *token.TableLoader
	tokenBlockLoader *token.BlockLoader
}

func (ti *sealedTokenIndex) GetValByTID(tid uint32) []byte {
	tokenTable := ti.tokenTableLoader.Load()
	if entry := tokenTable.GetEntryByTID(tid); entry != nil {
		block := ti.tokenBlockLoader.Load(entry.BlockIndex)
		return block.GetToken(entry.GetIndexInTokensBlock(tid))
	}
	return nil
}

func (ti *sealedTokenIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	field := parser.GetField(t)
	searchStr := parser.GetHint(t)

	tokenTable := ti.tokenTableLoader.Load()
	entries := tokenTable.SelectEntries(field, searchStr)
	if len(entries) == 0 {
		return nil, nil
	}

	tp := token.NewProvider(ti.tokenBlockLoader, entries)

	tids, err := pattern.Search(ti.ctx, t, tp)
	if err != nil {
		return nil, fmt.Errorf("search error: %s field: %s, query: %s", err, field, searchStr)
	}
	return tids, nil
}

func (ti *sealedTokenIndex) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	if order.IsReverse() {
		getBlockIndex = func(tid uint32) uint32 { return ti.lidsTable.GetLastBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorAsc)(lids.NewLIDsCursor(ti.lidsTable, ti.lidsLoader, startIndex, tid, stats, minLID, maxLID))
		}
	} else {
		getBlockIndex = func(tid uint32) uint32 { return ti.lidsTable.GetFirstBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorDesc)(lids.NewLIDsCursor(ti.lidsTable, ti.lidsLoader, startIndex, tid, stats, minLID, maxLID))
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

type sealedFetchIndex struct {
	idsIndex      *sealedIDsIndex
	docsReader    *storage.DocsReader
	blocksOffsets []uint64
}

func (fi *sealedFetchIndex) GetBlocksOffsets(num uint32) uint64 {
	return fi.blocksOffsets[num]
}

func (fi *sealedFetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	return fi.getDocPosByLIDs(fi.findLIDs(ids))
}

func (fi *sealedFetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return fi.docsReader.ReadDocs(blockOffset, docOffsets)
}

// findLIDs returns a slice of LIDs. If seq.ID is not found, LID has the value 0 at the corresponding position
func (fi *sealedFetchIndex) findLIDs(ids []seq.ID) []seq.LID {
	res := make([]seq.LID, len(ids))

	// left and right it is search range
	left := 1                      // first
	right := fi.idsIndex.Len() - 1 // last

	for i, id := range ids {

		if i == 0 || !seq.Less(id, ids[i-1]) {
			// reset search range (it is not DESC sorted IDs)
			left = 1
		}

		lid := seq.LID(util.BinSearchInRange(left, right, func(lid int) bool {
			return fi.idsIndex.LessOrEqual(seq.LID(lid), id)
		}))

		if id.MID == fi.idsIndex.GetMID(lid) && id.RID == fi.idsIndex.GetRID(lid) {
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
func (fi *sealedFetchIndex) getDocPosByLIDs(localIDs []seq.LID) []seq.DocPos {
	res := make([]seq.DocPos, len(localIDs))
	for i, lid := range localIDs {
		if lid == 0 {
			res[i] = seq.DocPosNotFound
			continue
		}
		res[i] = fi.idsIndex.docPos(lid)
	}
	return res
}

type sealedSearchIndex struct {
	*sealedIDsIndex
	*sealedTokenIndex
}
