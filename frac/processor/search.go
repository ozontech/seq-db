package processor

import (
	"context"
	"math"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/lids"
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
	minLID, maxLID := getLIDsBorders(params.From, params.To, index)
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
			aggs[i], err = evalAgg(index, query, sw, stats, minLID, maxLID, aggLimits, params.Order)
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

	var aggsResult []seq.QPRHistogram
	if len(params.AggQ) > 0 {
		aggsResult = make([]seq.QPRHistogram, len(aggs))
		m = sw.Start("agg_node_make_map")
		for i := range aggs {
			aggsResult[i], err = aggs[i].Aggregate()
			if err != nil {
				m.Stop()
				return nil, err
			}
			if len(aggsResult[i].HistogramByToken) > aggLimits.MaxGroupTokens && aggLimits.MaxGroupTokens > 0 {
				return nil, consts.ErrTooManyUniqValues
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
		Histogram: histogram,
	}

	stats.UpdateMetrics()

	return qpr, nil
}

func iterateEvalTree(
	ctx context.Context,
	params SearchParams,
	idsIndex idsIndex,
	evalTree node.Node,
	aggs []Aggregator,
	sw *stopwatch.Stopwatch,
) (int, seq.IDSources, map[seq.MID]uint64, error) {
	hasHist := params.HasHist()
	needScanAllRange := params.IsScanAllRequest()

	var histogram map[seq.MID]uint64
	if hasHist {
		histogram = make(map[seq.MID]uint64)
	}

	total := 0
	ids := seq.IDSources{}
	var lastID seq.ID

	for {

		if util.IsCancelled(ctx) {
			return total, ids, histogram, ctx.Err()
		}

		needMore := len(ids) < params.Limit
		if !needMore && !needScanAllRange {
			break
		}

		m := sw.Start("eval_tree_next")
		lid, has := evalTree.Next()
		m.Stop()

		if !has {
			break
		}

		if needMore || hasHist {
			m = sw.Start("get_mid")
			mid := idsIndex.GetMID(seq.LID(lid))
			m.Stop()

			if hasHist {
				bucket := mid
				bucket -= bucket % seq.MID(params.HistInterval)
				histogram[bucket]++
			}

			if needMore {
				m = sw.Start("get_rid")
				rid := idsIndex.GetRID(seq.LID(lid))
				m.Stop()

				id := seq.ID{MID: mid, RID: rid}

				if total == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
					ids = append(ids, seq.IDSource{ID: id})
				}
				lastID = id
			}
		}

		total++ // increment found counter, use aggNode, calculate histogram and collect ids only if id in borders

		if len(aggs) > 0 {
			m = sw.Start("agg_node_count")
			for i := range aggs {
				if err := aggs[i].Next(lid); err != nil {
					return total, ids, histogram, err
				}
			}
			m.Stop()
		}

	}

	return total, ids, histogram, nil
}

func getLIDsBorders(minMID, maxMID seq.MID, idsIndex idsIndex) (uint32, uint32) {
	if idsIndex.Len() == 0 {
		return 0, 0
	}

	minID := seq.ID{MID: minMID, RID: 0}
	maxID := seq.ID{MID: maxMID, RID: math.MaxUint64}

	from := 1 // first ID is not accessible (lid == 0 is invalid value)
	to := idsIndex.Len() - 1

	if minMID > 0 { // decrementing minMID to make LessOrEqual work like Less
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
			Aggs:      make([]seq.QPRHistogram, len(params.AggQ)),
		}
	}
	qpr := qprs[0]
	if len(qprs) > 1 {
		seq.MergeQPRs(qpr, qprs[1:], params.Limit, seq.MID(params.HistInterval), params.Order)
	}
	return qpr
}
