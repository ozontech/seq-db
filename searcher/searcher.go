package searcher

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Conf struct {
	AggLimits AggLimits
}

type Searcher struct {
	sem chan struct{}
	cfg Conf
}

func New(maxWorkersNum int, cfg Conf) *Searcher {
	if maxWorkersNum <= 0 {
		logger.Panic("invalid workers value")
	}
	return &Searcher{
		sem: make(chan struct{}, maxWorkersNum),
		cfg: cfg,
	}
}

func (s *Searcher) SearchDocs(ctx context.Context, fracs []frac.Fraction, params Params) ([]*seq.QPR, error) {
	return s.searchDocsAsync(ctx, fracs, params)
}

func (s *Searcher) searchDocsAsync(ctx context.Context, fracs []frac.Fraction, params Params) ([]*seq.QPR, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error

	once := sync.Once{}
	wg := sync.WaitGroup{}
	qprs := make([]*seq.QPR, len(fracs))

loop:
	for i, frac := range fracs {
		select {
		case <-ctx.Done():
			once.Do(func() { err = ctx.Err() })
			break loop
		case s.sem <- struct{}{}: // acquire semaphore
			wg.Add(1)
			go func() {
				var fracErr error
				if qprs[i], fracErr = s.fracSearch(ctx, params, frac); fracErr != nil {
					once.Do(func() {
						err = fracErr
						cancel()
					})
				}
				<-s.sem // release semaphore
				wg.Done()
			}()
		}
	}

	wg.Wait()

	if err != nil {
		return nil, err
	}

	return qprs, nil
}

func (s *Searcher) fracSearch(ctx context.Context, params Params, f frac.Fraction) (_ *seq.QPR, err error) {
	defer func() {
		if panicData := util.RecoverToError(recover(), metric.StorePanics); panicData != nil {
			err = fmt.Errorf("internal error: search panicked on fraction %s, error=%w", f.Info().Name(), panicData)
		}
	}()

	// The index of the active fraction changes in parts and at a single moment in time may not be consistent.
	// So we can add new IDs to the index but update the range [from; to] with a delay.
	// Because of this, at the Search stage, we can get IDs that are outside the fraction range [from; to].
	//
	// Because of this, at the next Fetch stage, we may not find documents with such IDs, because we will ignore
	// the fraction whose range [from; to] does not contain this ID.
	//
	// To prevent this from happening, so that the Search stage and the Fetch stage work consistently,
	// we must limit the query range in accordance with the current fraction range [from; to].
	info := f.Info()
	params.From = max(params.From, info.From)
	params.To = min(params.To, info.To)

	dataProvider, release, ok := f.DataProvider(ctx)
	if !ok {
		metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
		return nil, nil
	}

	defer release()

	stats := &Stats{}
	sw := stopwatch.New()

	qpr, err := s.indexSearch(ctx, params, dataProvider, sw, stats)
	if err != nil {
		return nil, err
	}

	if qpr == nil { // it is possible for a suicided fraction for example
		metric.CountersTotal.WithLabelValues("empty_qpr").Inc()
		return nil, nil
	}

	qpr.IDs.ApplyHint(info.Name())

	stagesMetric := chooseStagesMetric(dataProvider.Type(), params.HasAgg(), params.HasHist())
	sw.Export(stagesMetric)
	stats.updateMetrics()

	return qpr, nil
}

func getLIDsBorders(minMID, maxMID seq.MID, idsIndex frac.IDsIndex) (uint32, uint32) {
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

func (s *Searcher) indexSearch(ctx context.Context, params Params, dp frac.DataProvider, sw *stopwatch.Stopwatch, stats *Stats) (*seq.QPR, error) {
	hasAgg := params.HasAgg()

	totalMetric := sw.Start("total")
	defer totalMetric.Stop()

	m := sw.Start("get_lids_borders")
	idsProvider := dp.IDsIndex()
	minLID, maxLID := getLIDsBorders(params.From, params.To, idsProvider)
	m.Stop()

	m = sw.Start("eval_leaf")
	evalTree, err := buildEvalTree(params.AST, minLID, maxLID, stats, params.Order.IsReverse(),
		func(token parser.Token) (node.Node, error) {
			return evalLeaf(dp, token, sw, stats, minLID, maxLID, params.Order)
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
	if hasAgg {
		m = sw.Start("eval_agg")
		for i, query := range params.AggQ {
			aggs[i], err = evalAgg(dp, query, sw, stats, minLID, maxLID, s.cfg.AggLimits, params.Order)
			if err != nil {
				m.Stop()
				return nil, err
			}
		}
		m.Stop()
	}

	m = sw.Start("iterate_eval_tree")
	total, ids, histogram, err := iterateEvalTree(ctx, params, idsProvider, evalTree, aggs, sw)
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
			if len(aggsResult[i].HistogramByToken) > s.cfg.AggLimits.MaxGroupTokens && s.cfg.AggLimits.MaxGroupTokens > 0 {
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

	return qpr, nil
}

func iterateEvalTree(
	ctx context.Context,
	params Params,
	idsProvider frac.IDsIndex,
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
			mid := idsProvider.GetMID(seq.LID(lid))
			m.Stop()

			if hasHist {
				bucket := mid
				bucket -= bucket % seq.MID(params.HistInterval)
				histogram[bucket]++
			}

			if needMore {
				m = sw.Start("get_rid")
				rid := idsProvider.GetRID(seq.LID(lid))
				m.Stop()

				id := seq.ID{MID: mid, RID: rid}

				if total == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
					foundID := seq.IDSource{ID: id}
					ids = append(ids, foundID)
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
