package frac

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/fetcher"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/searcher"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

var (
	fetcherActiveStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "active_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

	activeAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	activeHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	activeRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
)

type activeDataProvider struct {
	f        *Active
	ctx      context.Context
	idsIndex *activeIDsIndex
}

// getIDsIndex creates on demand and returns ActiveIDsIndex.
// Creation of inverser for ActiveIDsIndex is expensive operation
func (dp *activeDataProvider) createIDsIndex() *activeIDsIndex {
	mapping := dp.f.GetAllDocuments() // creation order is matter
	mids := dp.f.MIDs.GetVals()       // mids and rids should be created after mapping to ensure that
	rids := dp.f.RIDs.GetVals()       // they contain all the ids that mapping contains.

	inverser := newInverser(mapping, len(mids))

	return &activeIDsIndex{
		inverser: inverser,
		mids:     mids,
		rids:     rids,
	}
}

func (dp *activeDataProvider) getIDsIndex() *activeIDsIndex {
	if dp.idsIndex == nil {
		dp.idsIndex = dp.createIDsIndex()
	}
	return dp.idsIndex
}

func (dp *activeDataProvider) getSearchIndexes() []*activeSearchIndex {
	return []*activeSearchIndex{{
		Active:           dp.f,
		activeIDsIndex:   dp.createIDsIndex(),
		activeTokenIndex: &activeTokenIndex{ip: dp},
	}}
}

func (dp *activeDataProvider) getFetchIndexes() []*activeFetchIndex {
	return []*activeFetchIndex{{
		blocksOffsets: dp.f.DocBlocks.GetVals(),
		docsPositions: dp.f.DocsPositions,
		docsReader:    dp.f.docsReader,
	}}
}

func (dp *activeDataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	res := make([][]byte, len(ids))
	for _, index := range dp.getFetchIndexes() {
		if err := fetcher.IndexFetch(ids, sw, index, res); err != nil {
			return nil, err
		}
	}
	sw.Export(fetcherActiveStagesSeconds)
	return res, nil
}

func (dp *activeDataProvider) Search(ctx context.Context, params searcher.Params) (*seq.QPR, error) {
	s := searcher.New(ctx, params, dp.f.searchCfg.AggLimits, getActiveSearchMetric(params))
	for _, index := range dp.getSearchIndexes() {
		if !index.IsIntersecting(params.From, params.To) {
			continue
		}
		if err := s.IndexSearch(index); err != nil {
			return nil, err
		}
	}
	qpr := s.GetResult()
	qpr.IDs.ApplyHint(dp.f.Info().Name())
	return qpr, nil
}

func getActiveSearchMetric(params searcher.Params) *prometheus.HistogramVec {
	if params.HasAgg() {
		return activeAggSearchSec
	}
	if params.HasHist() {
		return activeHistSearchSec
	}
	return activeRegSearchSec
}

type activeSearchIndex struct {
	*Active
	*activeIDsIndex
	*activeTokenIndex
}

type activeIDsIndex struct {
	mids     []uint64
	rids     []uint64
	inverser *inverser
}

func (p *activeIDsIndex) GetMID(lid seq.LID) seq.MID {
	restoredLID := p.inverser.Revert(uint32(lid))
	return seq.MID(p.mids[restoredLID])
}

func (p *activeIDsIndex) GetRID(lid seq.LID) seq.RID {
	restoredLID := p.inverser.Revert(uint32(lid))
	return seq.RID(p.rids[restoredLID])
}

func (p *activeIDsIndex) Len() int {
	return p.inverser.Len()
}

func (p *activeIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	checkedMID := p.GetMID(lid)
	if checkedMID == id.MID {
		return p.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

type activeTokenIndex struct {
	ip *activeDataProvider
}

func (ti *activeTokenIndex) GetValByTID(tid uint32) []byte {
	return ti.ip.f.TokenList.GetValByTID(tid)
}

func (ti *activeTokenIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	return ti.ip.f.TokenList.FindPattern(ti.ip.ctx, t, nil)
}

func (ti *activeTokenIndex) GetLIDsFromTIDs(tids []uint32, _ lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	f := ti.ip.f
	inv := ti.ip.getIDsIndex().inverser

	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		tlids := f.TokenList.Provide(tid)
		unmapped := tlids.GetLIDs(f.MIDs, f.RIDs)
		inverse := inverseLIDs(unmapped, inv, minLID, maxLID)
		nodes = append(nodes, node.NewStatic(inverse, order.IsReverse()))
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

type activeFetchIndex struct {
	blocksOffsets []uint64
	docsPositions *DocsPositions
	docsReader    *disk.DocsReader
}

func (di *activeFetchIndex) GetBlocksOffsets(num uint32) uint64 {
	return di.blocksOffsets[num]
}

func (di *activeFetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	docsPos := make([]seq.DocPos, len(ids))
	for i, id := range ids {
		docsPos[i] = di.docsPositions.GetSync(id)
	}
	return docsPos
}

func (di *activeFetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return di.docsReader.ReadDocs(blockOffset, docOffsets)
}
