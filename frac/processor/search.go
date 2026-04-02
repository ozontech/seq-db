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

type LIDsIter interface {
	Lids(out []node.LID) []node.LID
	Len() int
}

var lidsBufPool = sync.Pool{
	New: func() any {
		return lidsBuf{
			lids: make([]node.LID, 0, maxLIDBufCap),
		}
	},
}

const maxLIDBufCap = 4096

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
	total, ids, histogram, aggs, err := iterateEvalTree(ctx, params, index, evalTreeIter, aggSupplier, sw)
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
	evalTree func(need int, buf lidsBuf) LIDsIter,
	aggSupplier func() ([]Aggregator, error),
	sw *stopwatch.Stopwatch,
) (int, seq.IDSources, []uint64, []Aggregator, error) {
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
	buf := lidsBufPool.Get().(lidsBuf)
	defer lidsBufPool.Put(buf)

	timerEval := sw.Timer("eval_tree_next")
	timerMID := sw.Timer("get_mid")
	timerRID := sw.Timer("get_rid")
	timerAgg := sw.Timer("agg_node_count")

	var aggs []Aggregator
	for {
		if util.IsCancelled(ctx) {
			return total, ids, histogram, aggs, ctx.Err()
		}

		needMore := len(ids) < params.Limit
		if !needMore && !needScanAllRange {
			break
		}
		need := params.Limit - len(ids)
		if needScanAllRange {
			need = math.MaxUint32
		}
		// limit how much we drain from eval tree for one-by-one flow. ignored for batched flow
		need = min(need, maxLIDBufCap)

		timerEval.Start()
		lidBatch := evalTree(need, buf)
		timerEval.Stop()

		if lidBatch.Len() == 0 {
			break
		}

		for _, lid := range lidBatch.Lids(buf.lids) {

			needMore = len(ids) < params.Limit
			if !needMore && !needScanAllRange {
				break
			}
			seqLID := lid.ToSeqLID()

			if needMore || hasHist {
				timerMID.Start()
				mid := idsIndex.GetMID(seqLID)
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
					rid := idsIndex.GetRID(seqLID)
					timerRID.Stop()

					id := seq.ID{MID: mid, RID: rid}

					if total == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
						ids = append(ids, seq.IDSource{ID: id})
					}
					lastID = id
				}
			}

			total++ // increment found counter, use aggNode, calculate histogram and collect ids only if id in borders

			if params.HasAgg() {
				if aggs == nil {
					var err error
					aggs, err = aggSupplier()
					if err != nil {
						return total, ids, histogram, nil, err
					}
				}

				timerAgg.Start()
				for i := range aggs {
					if err := aggs[i].Next(lid); err != nil {
						timerAgg.Stop()
						return total, ids, histogram, aggs, err
					}
				}
				timerAgg.Stop()
			}
		}
	}

	return total, ids, histogram, aggs, nil
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
