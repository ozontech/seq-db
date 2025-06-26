package frac

import (
	"context"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	ctx    context.Context
	config *Config
	info   *Info

	mids *UInt64s
	rids *UInt64s

	tokenList *TokenList

	blocksOffsets []uint64
	docsPositions *DocsPositions
	docsReader    *disk.DocsReader

	idsIndex *activeIDsIndex
}

func (dp *activeDataProvider) release() {
	if dp.idsIndex != nil {
		dp.idsIndex.inverser.Release()
	}
}

// getIDsIndex creates on demand and returns ActiveIDsIndex.
// Creation of inverser for ActiveIDsIndex is expensive operation
func (dp *activeDataProvider) getIDsIndex() *activeIDsIndex {
	if dp.idsIndex == nil {
		// creation order is matter
		mapping := dp.tokenList.GetAllTokenLIDs().GetLIDs(dp.mids, dp.rids)
		mids := dp.mids.GetVals() // mids and rids should be created after mapping to ensure that
		rids := dp.rids.GetVals() // they contain all the ids that mapping contains.
		dp.idsIndex = &activeIDsIndex{
			inverser: newInverser(mapping, len(mids)),
			mids:     mids,
			rids:     rids,
		}
	}
	return dp.idsIndex
}

func (dp *activeDataProvider) getTokenIndex() *activeTokenIndex {
	return &activeTokenIndex{
		ctx:       dp.ctx,
		mids:      dp.mids,
		rids:      dp.rids,
		tokenList: dp.tokenList,
		inverser:  dp.getIDsIndex().inverser,
	}
}

func (dp *activeDataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	defer sw.Export(fetcherActiveStagesSeconds)

	res := make([][]byte, len(ids))

	indexes := []activeFetchIndex{{
		blocksOffsets: dp.blocksOffsets,
		docsPositions: dp.docsPositions,
		docsReader:    dp.docsReader,
	}}

	for _, fi := range indexes {
		if err := processor.IndexFetch(ids, sw, &fi, res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (dp *activeDataProvider) Search(params processor.SearchParams) (*seq.QPR, error) {
	// The index of the active fraction changes in parts and at a single moment in time may not be consistent.
	// So we can add new IDs to the index but update the range [from; to] with a delay.
	// Because of this, at the Search stage, we can get IDs that are outside the fraction range [from; to].
	//
	// Because of this, at the next Fetch stage, we may not find documents with such IDs, because we will ignore
	// the fraction whose range [from; to] does not contain this ID.
	//
	// To prevent this from happening, so that the Search stage and the Fetch stage work consistently,
	// we must limit the query range in accordance with the current fraction range [from; to].
	params.From = max(params.From, dp.info.From)
	params.To = min(params.To, dp.info.To)

	aggLimits := processor.AggLimits(dp.config.Search.AggLimits)

	sw := stopwatch.New()
	defer sw.Export(getActiveSearchMetric(params))

	t := sw.Start("total")

	m := sw.Start("new_search_index")
	indexes := []activeSearchIndex{{
		activeIDsIndex:   dp.getIDsIndex(),
		activeTokenIndex: dp.getTokenIndex(),
	}}
	m.Stop()

	qprs := make([]*seq.QPR, 0, len(indexes))

	for _, si := range indexes {
		qpr, err := processor.IndexSearch(dp.ctx, params, &si, aggLimits, sw)
		if err != nil {
			return nil, err
		}
		qprs = append(qprs, qpr)
	}

	res := processor.MergeQPRs(qprs, params)
	res.IDs.ApplyHint(dp.info.Name())
	t.Stop()

	return res, nil
}

func getActiveSearchMetric(params processor.SearchParams) *prometheus.HistogramVec {
	if params.HasAgg() {
		return activeAggSearchSec
	}
	if params.HasHist() {
		return activeHistSearchSec
	}
	return activeRegSearchSec
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

type activeSearchIndex struct {
	*activeIDsIndex
	*activeTokenIndex
}

type activeTokenIndex struct {
	ctx       context.Context
	mids      *UInt64s
	rids      *UInt64s
	tokenList *TokenList
	inverser  *inverser
}

func (si *activeTokenIndex) GetValByTID(tid uint32) []byte {
	return si.tokenList.GetValByTID(tid)
}

func (si *activeTokenIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	return si.tokenList.FindPattern(si.ctx, t, nil)
}

func (si *activeTokenIndex) GetLIDsFromTIDs(tids []uint32, _ lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		tlids := si.tokenList.Provide(tid)
		unmapped := tlids.GetLIDs(si.mids, si.rids)
		inverse := inverseLIDs(unmapped, si.inverser, minLID, maxLID)
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
