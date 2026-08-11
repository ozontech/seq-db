package frac

import (
	"context"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type skipMaskProvider interface {
	GetIDsIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, func() error, error)
	GetIDsBitmapByFrac(fracName string, minLID, maxLID uint32) (*roaring.Bitmap, error)
	RemoveFrac(fracName string)
}

type sealedDataProvider struct {
	ctx    context.Context
	info   *common.Info
	config *Config

	idsTable    *seqids.Table
	idsProvider *seqids.Provider

	lidsTable  *lids.Table
	lidsLoader *lids.Loader

	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader

	blocksOffsets []uint64
	docsReader    *storage.DocsReader

	// fractionTypeLabel can be either 'sealed' or 'remote'.
	// This value is used in metrics to distinguish between operations over local and remote fractions.
	fractionTypeLabel string

	skipMaskProvider skipMaskProvider
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
		fracName:         dp.info.Name(),
		idsIndex:         dp.getIDsIndex(),
		docsReader:       dp.docsReader,
		blocksOffsets:    dp.blocksOffsets,
		skipMaskProvider: dp.skipMaskProvider,
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
		skipMaskProvider: dp.skipMaskProvider,
	}
}

func (dp *sealedDataProvider) release() {
	dp.idsProvider.Release()
}

func (dp *sealedDataProvider) Fetch(ids []seq.ID, noSkipMasks bool) ([][]byte, error) {
	sw := stopwatch.New()

	defer sw.Export(
		fetcherStagesSeconds,
		stopwatch.SetLabel("fraction_type", dp.fractionTypeLabel),
	)

	res := make([][]byte, len(ids))
	if err := processor.IndexFetch(ids, noSkipMasks, sw, dp.getFetchIndex(), res); err != nil {
		return nil, err
	}

	return res, nil
}

func (dp *sealedDataProvider) Search(params processor.SearchParams) (*seq.QPR, error) {
	aggLimits := processor.AggLimits(dp.config.Search.AggLimits)
	queryOpt := processor.QueryOptimizationConfig{
		BatchExecution: processor.BatchExecutionConfig(dp.config.Search.QueryOptimization.BatchExecution),
	}

	// Limit the parameter range to data boundaries to prevent histogram overflow
	params.From = max(params.From, dp.info.From)
	params.To = min(params.To, dp.info.To)

	sw := stopwatch.New()

	defer sw.Export(
		fractionSearchMetric(params),
		stopwatch.SetLabel("fraction_type", dp.fractionTypeLabel),
	)

	t := sw.Start("total")
	defer t.Stop()

	qpr, err := processor.IndexSearch(dp.ctx, dp.info.BinaryDataVer, params, dp.getSearchIndex(), aggLimits, queryOpt, sw)
	if err != nil {
		return nil, err
	}
	qpr.IDs.ApplyHint(dp.info.Name())

	return qpr, nil
}

func (dp *sealedDataProvider) FindLIDs(ids []seq.ID) ([]seq.LID, error) {
	return dp.getFetchIndex().findLIDs(ids), nil
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

func (ii *sealedIDsIndex) GetMIDs(lidsBatch []node.LID, out []seq.MID) []seq.MID {
	mids, err := ii.provider.MIDs(lidsBatch, out)
	if err != nil {
		logger.Panic("get mids error", zap.String("frac", ii.fracName), zap.Int("lids_count", len(lidsBatch)), zap.Error(err))
	}
	return mids
}

func (ii *sealedIDsIndex) GetRID(lid seq.LID) seq.RID {
	rid, err := ii.provider.RID(lid)
	if err != nil {
		logger.Panic("get rid error", zap.String("frac", ii.fracName), zap.Uint32("lid", uint32(lid)), zap.Error(err))
	}
	return rid
}

func (ii *sealedIDsIndex) GetRIDs(lidsBatch []node.LID, out []seq.RID) []seq.RID {
	rids, err := ii.provider.RIDs(lidsBatch, out)
	if err != nil {
		logger.Panic("get rid error", zap.String("frac", ii.fracName), zap.Int("lids_count", len(lidsBatch)), zap.Error(err))
	}
	return rids
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

func (ti *sealedTokenIndex) GetValByTID(tid uint32, field string) []byte {
	tokenTable := ti.tokenTableLoader.Load()
	if entry := tokenTable.GetEntryByTID(tid, field); entry != nil {
		block := ti.tokenBlockLoader.GetTokenBlock(entry.BlockIndex)
		return block.GetToken(entry.GetIndexInTokensBlock(tid))
	}
	return nil
}

func (ti *sealedTokenIndex) GetTIDsByField(field string) ([]uint32, error) {
	table := ti.tokenTableLoader.Load()

	entries := table.SelectEntries(field, "")
	if len(entries) == 0 {
		return nil, nil
	}

	first := entries[0].StartTID
	last := entries[len(entries)-1].GetLastTID()

	tids := make([]uint32, (last-first)+1)
	for i := range tids {
		tids[i] = first + uint32(i)
	}

	return tids, nil
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

func (ti *sealedTokenIndex) GetFreqsByTIDs(tids []uint32, field string) []uint32 {
	freqs := make([]uint32, len(tids))
	if len(tids) == 0 {
		return freqs
	}

	tokenTable := ti.tokenTableLoader.Load()
	for i, tid := range tids {
		if tid == 0 {
			continue
		}
		entry := tokenTable.GetEntryByTID(tid, field)
		block := ti.tokenBlockLoader.Load(entry.BlockIndex)
		freqs[i] = block.GetFreq(entry.GetIndexInTokensBlock(tid))
	}
	return freqs
}

func (ti *sealedTokenIndex) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	if order.IsReverse() {
		getBlockIndex = func(tid uint32) uint32 { return ti.lidsTable.GetLastBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return lids.NewIteratorAsc(ti.lidsTable, ti.lidsLoader, startIndex, tid, stats, minLID, maxLID)
		}
	} else {
		getBlockIndex = func(tid uint32) uint32 { return ti.lidsTable.GetFirstBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return lids.NewIteratorDesc(ti.lidsTable, ti.lidsLoader, startIndex, tid, stats, minLID, maxLID)
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

func (ti *sealedTokenIndex) GetBatchedLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.BatchedNode {
	var (
		getBlockIndex          func(tid uint32) uint32
		getBatchedLIDsIterator func(uint32, uint32) node.BatchedNode
	)

	if order.IsReverse() {
		getBlockIndex = func(tid uint32) uint32 { return ti.lidsTable.GetLastBlockIndexForTID(tid) }
		getBatchedLIDsIterator = func(startIndex uint32, tid uint32) node.BatchedNode {
			return lids.NewBatchedIteratorAsc(lids.NewIteratorAsc(ti.lidsTable, ti.lidsLoader, startIndex, tid, stats, minLID, maxLID))
		}
	} else {
		getBlockIndex = func(tid uint32) uint32 { return ti.lidsTable.GetFirstBlockIndexForTID(tid) }
		getBatchedLIDsIterator = func(startIndex uint32, tid uint32) node.BatchedNode {
			return lids.NewBatchedIteratorDesc(lids.NewIteratorDesc(ti.lidsTable, ti.lidsLoader, startIndex, tid, stats, minLID, maxLID))
		}
	}

	startIndexes := make([]uint32, len(tids))
	for i, tid := range tids {
		startIndexes[i] = getBlockIndex(tid)
	}

	nodes := make([]node.BatchedNode, len(tids))
	for i, tid := range tids {
		nodes[i] = getBatchedLIDsIterator(startIndexes[i], tid)
	}

	return nodes
}

type sealedFetchIndex struct {
	fracName         string
	idsIndex         *sealedIDsIndex
	docsReader       *storage.DocsReader
	blocksOffsets    []uint64
	skipMaskProvider skipMaskProvider
}

func (fi *sealedFetchIndex) GetBlocksOffsets(num uint32) uint64 {
	return fi.blocksOffsets[num]
}

func (fi *sealedFetchIndex) GetDocPos(ids []seq.ID, noSkipMasks bool) ([]seq.DocPos, error) {
	allLids := fi.findLIDs(ids)

	if noSkipMasks {
		return fi.getDocPosByLIDs(allLids), nil
	}

	minLID, maxLID := uint32(0), uint32(math.MaxUint32)
	if len(allLids) > 0 {
		// allLids can be not sorted
		minVal, maxVal := allLids[0], allLids[0]
		for i := 1; i < len(allLids); i++ {
			minVal = min(minVal, allLids[i])
			maxVal = max(maxVal, allLids[i])
		}
		minLID, maxLID = uint32(minVal), uint32(maxVal)
	}

	skipLIDsBitmap, err := fi.skipMaskProvider.GetIDsBitmapByFrac(fi.fracName, minLID, maxLID)
	if err != nil {
		return nil, err
	}

	if skipLIDsBitmap == nil {
		return fi.getDocPosByLIDs(allLids), nil
	}

	for i, lid := range allLids {
		if skipLIDsBitmap.Contains(uint32(lid)) {
			allLids[i] = 0
		}
	}

	return fi.getDocPosByLIDs(allLids), nil
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

		// In case when ID does not exist in fraction, binary search will return `right + 1`.
		// Such value will correspond to the amount of LIDs in fraction, not to the index.
		if lid <= seq.LID(right) && id.MID == fi.idsIndex.GetMID(lid) && id.RID == fi.idsIndex.GetRID(lid) {
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
	skipMaskProvider skipMaskProvider
}

func (si *sealedSearchIndex) GetSkipLIDs(minLID, maxLID uint32, reverse bool) (node.Node, bool, func() error, error) {
	return si.skipMaskProvider.GetIDsIteratorByFrac(si.fracName, minLID, maxLID, reverse)
}
