package processor

import (
	"context"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/logger"
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

type LIDsIter interface {
	Lids(out []node.LID) []node.LID
	Len() int
}

var lidsBufPool = sync.Pool{
	New: func() any {
		return lidsBuf{
			// Currently, we drain up to 4k lids from eval tree, but with proper batching enabled
			// we can get as much as whole LID block can have (currently, 64k lids)
			lids: make([]node.LID, 0, consts.LIDBlockCap),
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
) (*seq.QPR, error) {
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
	if err != nil {
		return nil, err
	}

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
	skipLIDs, hasSkipLIDs, err := index.GetSkipLIDs(minLID, maxLID, params.Order.IsReverse())
	m.Stop()
	if err != nil {
		return nil, err
	}

	if hasSkipLIDs {
		m = sw.Start("eval_skip_lids")
		evalTree = evalSkipLIDs(evalTree, skipLIDs, stats)
		m.Stop()
	}

	var evalTreeIter func(need int, out lidsBuf) LIDsIter
	batchNode, ok := tryConvertToBatchedTree(evalTree)

	if ok {
		evalTreeIter = func(need int, _ lidsBuf) LIDsIter {
			// batched flow: juts get a batch and return
			return batchNode.NextBatch()
		}
	} else {
		evalTreeIter = func(need int, buf lidsBuf) LIDsIter {
			// iterator flow: buffer LIDs one by one and return a batch
			for i := 0; i < need; i++ {
				lid := evalTree.Next()
				if lid.IsNull() {
					break
				}
				buf = buf.append(lid)
			}
			return buf
		}
	}

	m = sw.Start("iterate_eval_tree")
	total, ids, histMap, aggs, err := iterateEvalTree(ctx, params, index, evalTreeIter, aggSupplier, sw)
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

	qpr := &seq.QPR{
		IDs:       ids,
		Aggs:      aggsResult,
		Total:     uint64(total),
		Histogram: histMap.ToMap(),
	}

	stats.UpdateMetrics()

	return qpr, nil
}

func iterateEvalTree(
	ctx context.Context,
	params SearchParams,
	idsIndex idsIndex,
	evalTree func(need int, buf lidsBuf) LIDsIter,
	aggSupplier func() ([]Aggregator, error),
	sw *stopwatch.Stopwatch,
) (int, seq.IDSources, HistMap, []Aggregator, error) {
	hasHist := params.HasHist()
	needScanAllRange := params.IsScanAllRequest()

	var hist HistMap
	if hasHist {
		hist = NewHistMap(params.From, params.To, params.HistInterval)
	}

	total := 0
	ids := seq.IDSources{}
	var lastID seq.ID
	buf := lidsBufPool.Get().(lidsBuf)
	defer lidsBufPool.Put(buf)
	mids := make([]seq.MID, 0, 4096)
	rids := make([]seq.RID, 0, 4096)

	timerEval := sw.Timer("eval_tree_next")
	timerMID := sw.Timer("get_mid")
	filterMIDs := sw.Timer("filter_mids")
	updateHist := sw.Timer("update_hist")
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
		needLids := params.Limit - len(ids)
		if needScanAllRange {
			needLids = math.MaxUint32
		}
		// limit how much we drain from eval tree for one-by-one flow. ignored for batched flow
		needLids = min(needLids, maxLidsToDrain)

		timerEval.Start()
		lidBatch := evalTree(needLids, buf)
		timerEval.Stop()

		if lidBatch.Len() == 0 {
			break
		}

		lidsSlice := lidBatch.Lids(buf.lids)

		needMids := min(params.Limit-len(ids), len(lidsSlice))
		if hasHist {
			// need to fetch mids for all lids for hist
			needMids = len(lidsSlice)
		}

		// Get MIDs
		if needMids > 0 {
			timerMID.Start()
			mids = idsIndex.GetMIDs(lidsSlice[0:needMids], mids[:0])
			timerMID.Stop()
		}

		// Filter out-of-range MIDs (only for hists)
		if hasHist {
			filterMIDs.Start()
			mids, lidsSlice = filterOutOfRangeMIDs(params, mids, lidsSlice)
			filterMIDs.Stop()
		}

		// Get RIDs
		// compute number of ids we can get here, since some MIDs might have been filtered out
		needIds := min(params.Limit-len(ids), len(lidsSlice))
		if needIds > 0 {
			timerRID.Start()
			rids = idsIndex.GetRIDs(lidsSlice[0:needIds], rids[:0])
			timerRID.Stop()
		}

		// Fill IDs for search
		for i := 0; i < needIds; i++ {
			id := seq.ID{MID: mids[i], RID: rids[i]}

			if i == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
				ids = append(ids, seq.IDSource{ID: id})
			}
			lastID = id
		}

		// Update hist map
		if hasHist {
			updateHist.Start()
			hist.Update(mids)
			updateHist.Stop()
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

func filterOutOfRangeMIDs(params SearchParams, mids []seq.MID, lidsSlice []node.LID) ([]seq.MID, []node.LID) {
	// most of the time we will never filter out any MIDs, therefore it's faster just to loop through and exit
	needFilter := false
	for i := 0; i < len(mids); i++ {
		// TODO(cheb0): filter with arrow?
		if mids[i] < params.From || mids[i] > params.To {
			needFilter = true
			break
		}
	}

	if needFilter {
		writeIdx := 0
		filteredOut := 0
		for i := 0; i < len(mids); i++ {
			if mids[i] < params.From || mids[i] > params.To {
				logger.Error("MID value outside the query range",
					zap.Time("from", params.From.Time()),
					zap.Time("to", params.To.Time()),
					zap.Time("mid", mids[i].Time()))
				filteredOut++
				continue
			} else {
				lidsSlice[writeIdx] = lidsSlice[i]
				mids[writeIdx] = mids[i]
				writeIdx++
			}
		}
		lidsSlice = lidsSlice[0 : len(lidsSlice)-filteredOut]
		mids = mids[0 : len(mids)-filteredOut]
	}
	return mids, lidsSlice
}

// lidsBuf maintains node.LID in slice as is (append order).
// Used to drain batches of LIDs when eval tree doesn't support batching.
type lidsBuf struct {
	lids []node.LID
}

func (b lidsBuf) append(x node.LID) lidsBuf {
	return lidsBuf{
		lids: append(b.lids, x),
	}
}

func (b lidsBuf) Len() int {
	return len(b.lids)
}

func (b lidsBuf) Lids(_ []node.LID) []node.LID {
	return b.lids
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
