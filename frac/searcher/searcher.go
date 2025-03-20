package searcher

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

type Searcher struct {
	ctx       context.Context
	params    Params
	aggLimits AggLimits
	metric    *prometheus.HistogramVec

	sw    *stopwatch.Stopwatch
	total stopwatch.Metric
	stats *stats
	qprs  []*seq.QPR
}

func New(ctx context.Context, params Params, aggLimits AggLimits, metric *prometheus.HistogramVec) *Searcher {
	s := Searcher{
		ctx:       ctx,
		params:    params,
		aggLimits: aggLimits,
		metric:    metric,

		sw:    stopwatch.New(),
		stats: &stats{},
	}

	s.total = s.sw.Start("total")
	return &s
}

func (s *Searcher) GetResult() *seq.QPR {
	qpr := mergeQPRs(s.qprs, s.params)

	s.total.Stop()
	s.sw.Export(s.metric)
	s.stats.UpdateMetrics()

	s.stats = &stats{}
	s.qprs = nil

	return qpr
}

func mergeQPRs(qprs []*seq.QPR, params Params) *seq.QPR {
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

func (s *Searcher) IndexSearch(index searchIndex) error {
	m := s.sw.Start("get_lids_borders")
	minLID, maxLID := getLIDsBorders(s.params.From, s.params.To, index)
	m.Stop()

	m = s.sw.Start("eval_leaf")
	evalTree, err := buildEvalTree(s.params.AST, minLID, maxLID, s.stats, s.params.Order.IsReverse(),
		func(token parser.Token) (node.Node, error) {
			return evalLeaf(index, token, s.sw, s.stats, minLID, maxLID, s.params.Order)
		},
	)
	m.Stop()

	if err != nil {
		return err
	}

	defer func(start time.Time) { s.stats.TreeDuration += time.Since(start) }(time.Now())

	if util.IsCancelled(s.ctx) {
		return s.ctx.Err()
	}

	aggs := make([]Aggregator, len(s.params.AggQ))
	if s.params.HasAgg() {
		m = s.sw.Start("eval_agg")
		for i, query := range s.params.AggQ {
			aggs[i], err = evalAgg(index, query, s.sw, s.stats, minLID, maxLID, s.aggLimits, s.params.Order)
			if err != nil {
				m.Stop()
				return err
			}
		}
		m.Stop()
	}

	m = s.sw.Start("iterate_eval_tree")
	total, ids, histogram, err := s.iterateEvalTree(index, evalTree, aggs)
	m.Stop()

	if err != nil {
		return err
	}

	s.stats.HitsTotal += total

	var aggsResult []seq.QPRHistogram
	if len(s.params.AggQ) > 0 {
		aggsResult = make([]seq.QPRHistogram, len(aggs))
		m = s.sw.Start("agg_node_make_map")
		for i := range aggs {
			aggsResult[i], err = aggs[i].Aggregate()
			if err != nil {
				m.Stop()
				return err
			}
			if len(aggsResult[i].HistogramByToken) > s.aggLimits.MaxGroupTokens && s.aggLimits.MaxGroupTokens > 0 {
				return consts.ErrTooManyUniqValues
			}
		}
		m.Stop()
	}

	if !s.params.WithTotal {
		total = 0
	}

	qpr := &seq.QPR{
		IDs:       ids,
		Aggs:      aggsResult,
		Total:     uint64(total),
		Histogram: histogram,
	}

	s.qprs = append(s.qprs, qpr)

	return nil
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

func (s *Searcher) iterateEvalTree(
	idsIndex idsIndex,
	evalTree node.Node,
	aggs []Aggregator,
) (int, seq.IDSources, map[seq.MID]uint64, error) {
	hasHist := s.params.HasHist()
	needScanAllRange := s.params.IsScanAllRequest()

	var histogram map[seq.MID]uint64
	if hasHist {
		histogram = make(map[seq.MID]uint64)
	}

	total := 0
	ids := seq.IDSources{}
	var lastID seq.ID

	for {

		if util.IsCancelled(s.ctx) {
			return total, ids, histogram, s.ctx.Err()
		}

		needMore := len(ids) < s.params.Limit
		if !needMore && !needScanAllRange {
			break
		}

		m := s.sw.Start("eval_tree_next")
		lid, has := evalTree.Next()
		m.Stop()

		if !has {
			break
		}

		if needMore || hasHist {
			m = s.sw.Start("get_mid")
			mid := idsIndex.GetMID(seq.LID(lid))
			m.Stop()

			if hasHist {
				bucket := mid
				bucket -= bucket % seq.MID(s.params.HistInterval)
				histogram[bucket]++
			}

			if needMore {
				m = s.sw.Start("get_rid")
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
			m = s.sw.Start("agg_node_count")
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
