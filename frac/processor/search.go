package processor

import (
	"context"
	"math"
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
	GetRID(seq.LID) seq.RID
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
}

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
	m.Stop()

	if err != nil {
		return nil, err
	}

	defer func(start time.Time) { stats.TreeDuration += time.Since(start) }(time.Now())

	if util.IsCancelled(ctx) {
		return nil, ctx.Err()
	}

	aggs := make([]Aggregator, len(params.AggQ))
	if params.HasAgg() {
		m = sw.Start("eval_agg")
		for i, query := range params.AggQ {
			aggs[i], err = evalAgg(
				index, query, sw, stats, minLID, maxLID, aggLimits,
				provideExtractTimeFunc(sw, index, query.Interval), params.Order,
			)
			if err != nil {
				m.Stop()
				return nil, err
			}
		}
		m.Stop()
	}

	m = sw.Start("iterate_eval_tree")
	total, ids, histogram, err := iterateEvalTree(ctx, params, index, evalTree, aggs, sw)
	m.Stop()

	if err != nil {
		return nil, err
	}

	stats.HitsTotal += total

	var aggsResult []seq.AggregatableSamples
	if len(params.AggQ) > 0 {
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
		Histogram: convertHistToMap(params, histogram),
	}

	stats.UpdateMetrics()

	return qpr, nil
}

func convertHistToMap(params SearchParams, hist []uint64) map[seq.MID]uint64 {
	if len(hist) == 0 {
		return nil
	}
	res := make(map[seq.MID]uint64, len(hist))
	histIntervalMID := seq.MillisToMID(params.HistInterval)
	bucket := params.From - params.From%histIntervalMID
	for _, cnt := range hist {
		if cnt > 0 {
			res[bucket] = cnt
		}
		bucket += histIntervalMID
	}
	return res
}

func iterateEvalTree(
	ctx context.Context,
	params SearchParams,
	idsIndex idsIndex,
	evalTree node.Node,
	aggs []Aggregator,
	sw *stopwatch.Stopwatch,
) (int, seq.IDSources, []uint64, error) {
	hasHist := params.HasHist()
	needScanAllRange := params.IsScanAllRequest()

	var (
		histBase     uint64
		histogram    []uint64
		histInterval seq.MID
	)
	if hasHist {
		histInterval = seq.MillisToMID(params.HistInterval)
		histBase = uint64(params.From) / uint64(histInterval)
		histSize := uint64(params.To)/uint64(histInterval) - histBase + 1
		histogram = make([]uint64, histSize)
	}

	total := 0
	ids := seq.IDSources{}
	var lastID seq.ID

	timerEval := sw.Timer("eval_tree_next")
	timerMID := sw.Timer("get_mid")
	timerRID := sw.Timer("get_rid")
	timerAgg := sw.Timer("agg_node_count")

	for i := 0; ; i++ {
		if i&1023 == 0 && util.IsCancelled(ctx) {
			return total, ids, histogram, ctx.Err()
		}

		needMore := len(ids) < params.Limit
		if !needMore && !needScanAllRange {
			break
		}

		timerEval.Start()
		lid, has := evalTree.Next()
		timerEval.Stop()

		if !has {
			break
		}

		if needMore || hasHist {
			timerMID.Start()
			mid := idsIndex.GetMID(seq.LID(lid))
			timerMID.Stop()

			if hasHist {
				if mid < params.From || mid > params.To {
					logger.Error("MID value outside the query range",
						zap.Time("from", params.From.Time()),
						zap.Time("to", params.To.Time()),
						zap.Time("mid", mid.Time()))
					continue
				}
				bucketIndex := uint64(mid)/uint64(histInterval) - histBase
				histogram[bucketIndex]++
			}

			if needMore {
				timerRID.Start()
				rid := idsIndex.GetRID(seq.LID(lid))
				timerRID.Stop()

				id := seq.ID{MID: mid, RID: rid}

				if total == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
					ids = append(ids, seq.IDSource{ID: id})
				}
				lastID = id
			}
		}

		total++ // increment found counter, use aggNode, calculate histogram and collect ids only if id in borders

		if len(aggs) > 0 {
			timerAgg.Start()
			for i := range aggs {
				if err := aggs[i].Next(lid); err != nil {
					timerAgg.Stop()
					return total, ids, histogram, err
				}
			}
			timerAgg.Stop()
		}

	}

	return total, ids, histogram, nil
}

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
