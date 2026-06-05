package processor

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// IDsIndex provide access to seq.ID by seq.LID
// where seq.LID (Local ID) is a position of seq.ID in sorted sequence.
// seq.ID sorted in descending order, so for seq.LID1 > seq.LID2
// we have seq.ID1 < seq.ID2
type idsIndex interface {
	// LessOrEqual checks if seq.ID in LID position less or equal searched seq.ID, i.e. seqID(lid) <= id
	LessOrEqual(lid seq.LID, id seq.ID) bool
	GetMID(seq.LID) seq.MID
	GetMIDs(lids []node.LID, out []seq.MID) []seq.MID
	GetRID(seq.LID) seq.RID
	GetRIDs(lids []node.LID, out []seq.RID) []seq.RID
	Len() int
}

type tokenIndex interface {
	GetValByTID(tid uint32) []byte
	GetTIDsByTokenExpr(token parser.Token) ([]uint32, error)
	GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node
}

type searchIndex interface {
	tokenIndex
	idsIndex
	GetSkipLIDs(minLID, maxLID uint32, reverse bool) (node.Node, bool, error)
}

type searchBuffers struct {
	lids []node.LID
	mids []seq.MID
	rids []seq.RID
}

var searchBuffersPool = sync.Pool{
	New: func() any {
		return &searchBuffers{
			// Currently, we drain up to 4k lids from eval tree, but with proper batching enabled
			// we can get as much as whole LID block can have (currently, 64k lids)
			lids: make([]node.LID, 0, consts.LIDBlockCap),
			mids: make([]seq.MID, 0, consts.LIDBlockCap),
			rids: make([]seq.RID, 0, consts.LIDBlockCap),
		}
	},
}

const maxLidsToDrain = 4096

func IndexSearch(
	ctx context.Context,
	params SearchParams,
	index searchIndex,
	aggLimits AggLimits,
	sw *stopwatch.Stopwatch,
) (qpr *seq.QPR, err error) {
	stats := &searchStats{}

	m := sw.Start("get_lids_borders")
	minLID, maxLID := getLIDsBorders(params, index)
	m.Stop()

	m = sw.Start("eval_leaf")
	evalTree, err := buildEvalTree(params.AST, minLID, maxLID, stats, params.Order.IsReverse(),
		func(token parser.Token) (node.Node, error) {
			return evalLeaf(index, token, sw, stats, minLID, maxLID, params.Order)
		},
	)
	m.Stop()

	if err != nil {
		return nil, err
	}

	defer func(start time.Time) { stats.TreeDuration += time.Since(start) }(time.Now())

	if util.IsCancelled(ctx) {
		return nil, ctx.Err()
	}

	var aggSupplier func() ([]Aggregator, error)

	if params.HasAgg() {
		aggSupplier = func() ([]Aggregator, error) {
			mAgg := sw.Start("eval_agg")
			defer mAgg.Stop()

			aggs := make([]Aggregator, len(params.AggQ))
			for i, query := range params.AggQ {
				aggs[i], err = evalAgg(
					index, query, sw, stats, minLID, maxLID, aggLimits,
					provideExtractTimeFunc(sw, index, query.Interval), params.Order,
				)
				if err != nil {
					return nil, err
				}
			}

			return aggs, nil
		}
	}

	m = sw.Start("get_skip_lids")
	skipLIDs, hasSkipLIDs, release, err := index.GetSkipLIDs(minLID, maxLID, params.Order.IsReverse())
	defer func() {
		err = errors.Join(err, release())
	}()
	m.Stop()
	if err != nil {
		return nil, err
	}

	if hasSkipLIDs {
		m = sw.Start("eval_skip_lids")
		evalTree = evalSkipLIDs(evalTree, skipLIDs, stats)
		m.Stop()
	}

	m = sw.Start("iterate_eval_tree")
	total, ids, histMap, aggs, err := iterateEvalTree(ctx, params, index, evalTree, aggSupplier, sw)
	m.Stop()

	if err != nil {
		return nil, err
	}

	stats.HitsTotal += total

	var aggsResult []seq.AggregatableSamples
	if len(aggs) > 0 {
		aggsResult = make([]seq.AggregatableSamples, len(aggs))
		m = sw.Start("agg_node_make_map")
		for i := range aggs {
			aggsResult[i], err = aggs[i].Aggregate()
			if err != nil {
				m.Stop()
				return nil, err
			}
			if len(aggsResult[i].SamplesByBin) > aggLimits.MaxGroupTokens && aggLimits.MaxGroupTokens > 0 {
				return nil, consts.ErrTooManyGroupTokens
			}
		}
		m.Stop()
	}

	if !params.WithTotal {
		total = 0
	}

	qpr = &seq.QPR{
		IDs:       ids,
		Aggs:      aggsResult,
		Total:     uint64(total),
		Histogram: histMap.ToMap(),
	}

	stats.UpdateMetrics()

	return qpr, nil
}

func batcher(evalTree node.Node, buf []node.LID) func(need int) []node.LID {
	if batchNode, ok := tryConvertToBatchedTree(evalTree); ok {
		return func(need int) []node.LID {
			buf = batchNode.NextBatch().LIDs(buf[:0])
			if len(buf) > need {
				buf = buf[:need]
			}
			return buf
		}
	}

	return func(need int) []node.LID {
		buf = buf[:0]
		for range min(maxLidsToDrain, need) {
			lid := evalTree.Next()
			if lid.IsNull() {
				break
			}
			buf = append(buf, lid)
		}
		return buf
	}
}

func iterateEvalTree(
	ctx context.Context,
	params SearchParams,
	idsIndex idsIndex,
	evalTree node.Node,
	aggSupplier func() ([]Aggregator, error),
	sw *stopwatch.Stopwatch,
) (int, seq.IDSources, HistMap, []Aggregator, error) {
	hasHist := params.HasHist()
	needScanAllRange := params.IsScanAllRequest()

	var hist HistMap
	if hasHist {
		hist = NewHistMap(params.From, params.To, params.HistInterval)
	}

	var (
		total  int
		lastID seq.ID
		ids    seq.IDSources
	)

	buffers := searchBuffersPool.Get().(*searchBuffers)
	defer searchBuffersPool.Put(buffers)
	mids := buffers.mids
	rids := buffers.rids

	batchedEvalTree := batcher(evalTree, buffers.lids)

	timerEval := sw.Timer("eval_tree_next")
	timerMID := sw.Timer("get_mid")
	timerUpdateHist := sw.Timer("update_hist")
	timerRID := sw.Timer("get_rid")
	timerAgg := sw.Timer("agg_node_count")

	var aggs []Aggregator
	for {
		if util.IsCancelled(ctx) {
			return total, ids, hist, aggs, ctx.Err()
		}

		needMore := len(ids) < params.Limit
		if !needMore && !needScanAllRange {
			break
		}
		needLIDs := params.Limit - len(ids)
		if needScanAllRange {
			needLIDs = math.MaxUint32
		}

		timerEval.Start()
		lidsSlice := batchedEvalTree(needLIDs)
		timerEval.Stop()

		if len(lidsSlice) == 0 {
			break
		}

		needMIDs := min(params.Limit-len(ids), len(lidsSlice))
		if hasHist {
			// need to fetch mids for all lids for hist
			needMIDs = len(lidsSlice)
		}

		// Get MIDs
		if needMIDs > 0 {
			timerMID.Start()
			mids = idsIndex.GetMIDs(lidsSlice[:needMIDs], mids[:0])
			timerMID.Stop()
		}

		// Get RIDs
		// compute number of ids we can get here, since some MIDs might have been filtered out
		needIDs := min(params.Limit-len(ids), len(lidsSlice))
		if needIDs > 0 {
			timerRID.Start()
			rids = idsIndex.GetRIDs(lidsSlice[0:needIDs], rids[:0])
			timerRID.Stop()
		}

		// Fill IDs for search
		for i := 0; i < needIDs; i++ {
			id := seq.ID{MID: mids[i], RID: rids[i]}

			if i == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
				ids = append(ids, seq.IDSource{ID: id})
			}
			lastID = id
		}

		// Update hist map
		if hasHist {
			timerUpdateHist.Start()
			hist.Update(mids)
			timerUpdateHist.Stop()
		}

		// Update aggregators
		if params.HasAgg() {
			if aggs == nil {
				var err error
				aggs, err = aggSupplier() // sw timer is activated inside aggSupplier
				if err != nil {
					return total, ids, hist, nil, err
				}
			}

			timerAgg.Start()
			for i := range aggs {
				for _, lid := range lidsSlice {
					if err := aggs[i].Next(lid); err != nil {
						timerAgg.Stop()
						return total, ids, hist, aggs, err
					}
				}
			}
			timerAgg.Stop()
		}

		total += len(lidsSlice)
	}

	return total, ids, hist, aggs, nil
}

func tryConvertToBatchedTree(evalTree node.Node) (node.BatchedNode, bool) {
	switch it := evalTree.(type) {
	case *lids.IteratorDesc:
		return it, true
	case *lids.IteratorAsc:
		return it, true
	default:
		return nil, false
	}
}

// getLIDsBorders return min and max LID borders (including) for search
func getLIDsBorders(params SearchParams, idsIndex idsIndex) (uint32, uint32) {
	if idsIndex.Len() == 0 {
		return 0, 0
	}
	minMID := params.From
	maxMID := params.To

	minID := seq.ID{MID: minMID, RID: 0}
	maxID := seq.ID{MID: maxMID, RID: math.MaxUint64}

	minIDFromOffset := false
	if uint64(params.OffsetId.MID) != 0 {
		if params.Order == seq.DocsOrderDesc && seq.Less(params.OffsetId, maxID) {
			// decrement RID by 1 to exclude already seen document while paging
			maxID = params.OffsetId.Dec()
		}
		if params.Order == seq.DocsOrderAsc && seq.Less(minID, params.OffsetId) {
			minID = params.OffsetId.Inc()
			minIDFromOffset = true
		}
	}

	from := 1 // first ID is not accessible (lid == 0 is invalid value)
	to := idsIndex.Len() - 1

	// decrementing minMID to make LessOrEqual work like Less
	// do not decrement minID if min ID comes from offset-id since we have to exclude the doc ID equal to offset-id
	if !minIDFromOffset && minMID > 0 {
		minID.MID--
		minID.RID = math.MaxUint64
	}

	// minLID corresponds to maxMID and maxLID corresponds to minMID due to reverse order of MIDs
	minLID := util.BinSearchInRange(from, to, func(lid int) bool { return idsIndex.LessOrEqual(seq.LID(lid), maxID) })
	maxLID := util.BinSearchInRange(minLID, to, func(lid int) bool { return idsIndex.LessOrEqual(seq.LID(lid), minID) }) - 1

	return uint32(minLID), uint32(maxLID)
}

func MergeQPRs(qprs []*seq.QPR, params SearchParams) *seq.QPR {
	if len(qprs) == 0 {
		return &seq.QPR{
			Histogram: make(map[seq.MID]uint64),
			Aggs:      make([]seq.AggregatableSamples, len(params.AggQ)),
		}
	}
	qpr := qprs[0]
	if len(qprs) > 1 {
		seq.MergeQPRs(qpr, qprs[1:], params.Limit, seq.MillisToMID(params.HistInterval), params.Order)
	}
	return qpr
}
