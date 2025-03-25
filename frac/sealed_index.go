package frac

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/fetcher"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/searcher"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type sealedDataProvider struct {
	f                *Sealed
	ctx              context.Context
	fracVersion      BinaryDataVersion
	midCache         *UnpackCache
	ridCache         *UnpackCache
	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader
}

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

func (dp *sealedDataProvider) getFetchIndex() *sealedFetchIndex {
	idsLoader := NewIDsLoader(dp.f.indexReader, dp.f.indexCache, dp.f.idsTable)
	docsIndex := &sealedFetchIndex{
		idsIndex: &sealedIDsIndex{
			loader:      idsLoader,
			midCache:    dp.midCache,
			ridCache:    dp.ridCache,
			fracVersion: dp.fracVersion,
		},
		idsLoader:     idsLoader,
		docsReader:    dp.f.docsReader,
		blocksOffsets: dp.f.BlocksOffsets,
	}
	return docsIndex
}

func (dp *sealedDataProvider) getSearchIndex() *sealedSearchIndex {
	return &sealedSearchIndex{
		sealedIDsIndex: &sealedIDsIndex{
			loader:      NewIDsLoader(dp.f.indexReader, dp.f.indexCache, dp.f.idsTable),
			midCache:    dp.midCache,
			ridCache:    dp.ridCache,
			fracVersion: dp.fracVersion,
		},
		SealedTokenIndex: &SealedTokenIndex{ip: dp},
	}
}

func (dp *sealedDataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	res := make([][]byte, len(ids))
	if err := fetcher.IndexFetch(ids, sw, dp.getFetchIndex(), res); err != nil {
		return nil, err
	}
	sw.Export(fetcherSealedStagesSeconds)

	return res, nil
}

func (dp *sealedDataProvider) Search(ctx context.Context, params searcher.Params) (*seq.QPR, error) {
	s := searcher.New(ctx, params, dp.f.searchCfg.AggLimits, getSealedSearchMetric(params))
	if err := s.IndexSearch(dp.getSearchIndex()); err != nil {
		return nil, err
	}
	qpr := s.GetResult()
	qpr.IDs.ApplyHint(dp.f.Info().Name())

	return qpr, nil
}

func getSealedSearchMetric(params searcher.Params) *prometheus.HistogramVec {
	if params.HasAgg() {
		return sealedAggSearchSec
	}
	if params.HasHist() {
		return sealedHistSearchSec
	}
	return sealedRegSearchSec
}

type sealedSearchIndex struct {
	*sealedIDsIndex
	*SealedTokenIndex
}

type sealedIDsIndex struct {
	loader      *IDsLoader
	midCache    *UnpackCache
	ridCache    *UnpackCache
	fracVersion BinaryDataVersion
}

func (p *sealedIDsIndex) GetMID(lid seq.LID) seq.MID {
	p.loader.GetMIDsBlock(seq.LID(lid), p.midCache)
	return seq.MID(p.midCache.GetValByLID(uint64(lid)))
}

func (p *sealedIDsIndex) GetRID(lid seq.LID) seq.RID {
	p.loader.GetRIDsBlock(seq.LID(lid), p.ridCache, p.fracVersion)
	return seq.RID(p.ridCache.GetValByLID(uint64(lid)))
}

func (p *sealedIDsIndex) Len() int {
	return int(p.loader.table.IDsTotal)
}

func (p *sealedIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
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

type SealedTokenIndex struct {
	ip *sealedDataProvider
}

func (ti *SealedTokenIndex) GetValByTID(tid uint32) []byte {
	tokenTable := ti.ip.tokenTableLoader.Load()
	if entry := tokenTable.GetEntryByTID(tid); entry != nil {
		return ti.ip.tokenBlockLoader.Load(entry).GetValByTID(tid)
	}
	return nil
}

func (ti *SealedTokenIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	field := parser.GetField(t)
	searchStr := parser.GetHint(t)

	tokenTable := ti.ip.tokenTableLoader.Load()
	entries := tokenTable.SelectEntries(field, searchStr)
	if len(entries) == 0 {
		return nil, nil
	}

	f := token.NewFetcher(ti.ip.tokenBlockLoader, entries)
	s := pattern.NewSearcher(t, f, f.GetTokensCount())

	tids := []uint32{}
	for i := s.Begin(); i <= s.End(); i++ {
		if util.IsCancelled(ti.ip.ctx) {
			err := fmt.Errorf("search cancelled when matching tokens: reason=%w field=%s, query=%s", ti.ip.ctx.Err(), field, searchStr)
			return nil, err
		}
		if s.Check(f.FetchToken(i)) {
			tids = append(tids, f.GetTIDFromIndex(i))
		}
	}
	return tids, nil
}

func (ti *SealedTokenIndex) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	f := ti.ip.f
	loader := lids.NewLoader(f.indexReader, f.indexCache.LIDs)

	if order.IsReverse() {
		getBlockIndex = func(tid uint32) uint32 { return f.lidsTable.GetLastBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorAsc)(lids.NewLIDsCursor(f.lidsTable, loader, startIndex, tid, stats, minLID, maxLID))
		}
	} else {
		getBlockIndex = func(tid uint32) uint32 { return f.lidsTable.GetFirstBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorDesc)(lids.NewLIDsCursor(f.lidsTable, loader, startIndex, tid, stats, minLID, maxLID))
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
	idsLoader     *IDsLoader
	docsReader    *disk.DocsReader
	blocksOffsets []uint64
}

func (di *sealedFetchIndex) GetBlocksOffsets(num uint32) uint64 {
	return di.blocksOffsets[num]
}

func (di *sealedFetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	return di.getDocPosByLIDs(di.findLIDs(ids))
}

func (di *sealedFetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return di.docsReader.ReadDocs(blockOffset, docOffsets)
}

// findLIDs returns a slice of LIDs. If seq.ID is not found, LID has the value 0 at the corresponding position
func (di *sealedFetchIndex) findLIDs(ids []seq.ID) []seq.LID {
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
func (di *sealedFetchIndex) getDocPosByLIDs(localIDs []seq.LID) []seq.DocPos {
	var (
		prevIndex int64
		positions []uint64
		startLID  seq.LID
	)

	res := make([]seq.DocPos, len(localIDs))
	for i, lid := range localIDs {
		if lid == 0 {
			res[i] = seq.DocPosNotFound
			continue
		}

		index := di.idsLoader.getIDBlockIndexByLID(lid)
		if positions == nil || prevIndex != index {
			positions = di.idsLoader.GetParamsBlock(uint32(index))
			startLID = seq.LID(index * consts.IDsPerBlock)
		}

		res[i] = seq.DocPos(positions[lid-startLID])
	}

	return res
}
