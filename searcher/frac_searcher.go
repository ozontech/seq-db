package searcher

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type fracSearcher struct {
	ctx    context.Context
	params Params
	limits AggLimits
}

func newFracSearcher(ctx context.Context, params Params, limits AggLimits) fracSearcher {
	return fracSearcher{
		ctx:    ctx,
		params: params,
		limits: limits,
	}
}

func (f *fracSearcher) Search(frac frac.Fraction) (_ []*seq.QPR, err error) {
	defer func() {
		if panicData := util.RecoverToError(recover(), metric.StorePanics); panicData != nil {
			err = fmt.Errorf("internal error: search panicked on fraction %s, error=%w", frac.Info().Name(), panicData)
		}
	}()

	info := frac.Info()
	ip, release := frac.TakeIndexes(f.ctx)
	defer release()

	indexes := ip.Indexes()
	if len(indexes) == 0 { // it is possible for a suicided fraction for example
		metric.CountersTotal.WithLabelValues("empty_qpr").Inc()
		return nil, nil
	}

	qprs := make([]*seq.QPR, len(indexes))
	fracName := frac.Info().Name()

	stats := &Stats{}
	sw := stopwatch.New()

	for i, index := range indexes {
		if !index.IsIntersecting(f.params.From, f.params.To) {
			continue
		}

		var err error
		total := sw.Start("search_index")
		qprs[i], err = f.indexSearch(index, info.From, info.To, sw, stats)
		total.Stop()

		if err != nil {
			return nil, err
		}

		qprs[i].IDs.ApplyHint(fracName)
	}

	sw.Export(chooseStagesMetric(frac.Type(), f.params.HasAgg(), f.params.HasHist()))
	stats.updateMetrics()

	return qprs, nil
}

func (f *fracSearcher) indexSearch(index frac.Index, minMID, maxMID seq.MID, sw *stopwatch.Stopwatch, stats *Stats) (*seq.QPR, error) {
	// The index of the active fraction changes in parts and at a single moment in time may not be consistent.
	// So we can add new IDs to the index but update the range [from; to] with a delay.
	// Because of this, at the Search stage, we can get IDs that are outside the fraction range [from; to].
	//
	// Because of this, at the next Fetch stage, we may not find documents with such IDs, because we will ignore
	// the fraction whose range [from; to] does not contain this ID.
	//
	// To prevent this from happening, so that the Search stage and the Fetch stage work consistently,
	// we must limit the query range in accordance with the current fraction range [from; to].
	from := max(f.params.From, minMID)
	to := min(f.params.To, maxMID)

	m := sw.Start("get_lids_borders")
	idsIndex := index.IDsIndex()
	minLID, maxLID := getLIDsBorders(from, to, idsIndex)
	m.Stop()

	m = sw.Start("eval_leaf")
	evalTree, err := buildEvalTree(f.params.AST, minLID, maxLID, stats, f.params.Order.IsReverse(),
		func(token parser.Token) (node.Node, error) {
			return evalLeaf(sw, index.TokenIndex(), token, stats, minLID, maxLID, f.params.Order)
		},
	)
	m.Stop()

	if err != nil {
		return nil, err
	}

	start := time.Now()

	if util.IsCancelled(f.ctx) {
		return nil, f.ctx.Err()
	}

	var aggs []Aggregator
	if f.params.HasAgg() {
		m = sw.Start("eval_agg")
		aggs, err = f.evalAggs(index.TokenIndex(), minLID, maxLID, sw, stats)
		m.Stop()

		if err != nil {
			return nil, err
		}
	}

	qpr := &seq.QPR{}
	m = sw.Start("iterate_eval_tree")
	qpr.Total, qpr.IDs, qpr.Histogram, err = f.iterateEvalTree(idsIndex, evalTree, aggs, sw)
	m.Stop()

	if err != nil {
		return nil, err
	}

	stats.HitsTotal += int(qpr.Total)
	if !f.params.WithTotal {
		qpr.Total = 0
	}

	if f.params.HasAgg() {
		m = sw.Start("agg_node_make_map")
		qpr.Aggs, err = f.aggregate(aggs)
		m.Stop()

		if err != nil {
			return nil, err
		}
	}

	stats.TreeDuration += time.Since(start)

	return qpr, nil
}

func getLIDsBorders(minMID, maxMID seq.MID, ids frac.IDsIndex) (uint32, uint32) {
	if ids.Len() == 0 {
		return 0, 0
	}

	minID := seq.ID{MID: minMID, RID: 0}
	maxID := seq.ID{MID: maxMID, RID: math.MaxUint64}

	from := 1 // first ID is not accessible (lid == 0 is invalid value)
	to := ids.Len() - 1

	if minMID > 0 { // decrementing minMID to make LessOrEqual work like Less
		minID.MID--
		minID.RID = math.MaxUint64
	}

	// minLID corresponds to maxMID and maxLID corresponds to minMID due to reverse order of MIDs
	minLID := util.BinSearchInRange(from, to, func(lid int) bool { return ids.LessOrEqual(seq.LID(lid), maxID) })
	maxLID := util.BinSearchInRange(minLID, to, func(lid int) bool { return ids.LessOrEqual(seq.LID(lid), minID) }) - 1

	return uint32(minLID), uint32(maxLID)
}

func (f *fracSearcher) evalAggs(ti frac.TokenIndex, minLID, maxLID uint32, sw *stopwatch.Stopwatch, stats *Stats) ([]Aggregator, error) {
	aggs := make([]Aggregator, len(f.params.AggQ))
	for i, query := range f.params.AggQ {
		var err error
		aggs[i], err = evalAgg(sw, ti, query, stats, minLID, maxLID, f.limits, f.params.Order)
		if err != nil {
			return nil, err
		}
	}
	return aggs, nil
}

func (f *fracSearcher) aggregate(aggs []Aggregator) ([]seq.QPRHistogram, error) {
	hist := make([]seq.QPRHistogram, len(aggs))
	for i := range aggs {
		var err error
		hist[i], err = aggs[i].Aggregate()
		if err != nil {
			return nil, err
		}
		if len(hist[i].HistogramByToken) > f.limits.MaxGroupTokens && f.limits.MaxGroupTokens > 0 {
			return nil, consts.ErrTooManyUniqValues
		}
	}
	return hist, nil
}

func (f *fracSearcher) iterateEvalTree(idsIndex frac.IDsIndex, evalTree node.Node, aggs []Aggregator, sw *stopwatch.Stopwatch) (uint64, seq.IDSources, map[seq.MID]uint64, error) {
	hasHist := f.params.HasHist()
	needScanAllRange := f.params.NeedScanAllRange()

	var histogram map[seq.MID]uint64
	if hasHist {
		histogram = make(map[seq.MID]uint64)
	}

	var (
		total  uint64
		lastID seq.ID
	)
	ids := make(seq.IDSources, 0, f.params.Limit)

	for {
		if util.IsCancelled(f.ctx) {
			return total, ids, histogram, f.ctx.Err()
		}

		needMore := len(ids) < f.params.Limit
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
				bucket -= bucket % seq.MID(f.params.HistInterval)
				histogram[bucket]++
			}

			if needMore {
				m = sw.Start("get_rid")
				rid := idsIndex.GetRID(seq.LID(lid))
				m.Stop()

				id := seq.ID{MID: mid, RID: rid}

				if total == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
					ids = append(ids, seq.IDSource{ID: id})
				} else {
					total--
				}
				lastID = id
			}
		}

		total++

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
