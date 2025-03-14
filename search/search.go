package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"

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

type AggQuery struct {
	Field     *parser.Literal
	GroupBy   *parser.Literal
	Func      seq.AggFunc
	Quantiles []float64
}

type Params struct {
	AST *parser.ASTNode

	AggQ         []AggQuery
	AggLimits    AggLimits
	HistInterval uint64

	From  seq.MID
	To    seq.MID
	Limit int

	WithTotal bool
	Order     seq.DocsOrder
}

func (p *Params) HasHist() bool {
	return p.HistInterval > 0
}

func (p *Params) HasAgg() bool {
	return len(p.AggQ) > 0
}

func (p *Params) IsScanAllRequest() bool {
	return p.WithTotal || p.HasAgg() || p.HasHist()
}

type fracResponse struct {
	qpr   *seq.QPR
	stats *Stats
	err   error
	frac  frac.Fraction
}

// Task is a search task for the worker.
type Task struct {
	Ctx    context.Context
	Frac   frac.Fraction
	Params Params
	Resp   chan fracResponse
}

// WorkerPool reuses memory and limits fraction requests.
type WorkerPool struct {
	workerTasks chan Task
}

// NewWorkerPool returns new WorkerPool.
func NewWorkerPool(workers int) *WorkerPool {
	if workers <= 0 {
		logger.Panic("invalid workers value")
	}

	s := &WorkerPool{
		workerTasks: make(chan Task, workers),
	}

	for i := 0; i < workers; i++ {
		go work(s.workerTasks)
	}

	return s
}

func (wp *WorkerPool) Search(ctx context.Context, fracs []frac.Fraction, params Params) ([]*seq.QPR, []*Stats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	respChan := make(chan fracResponse, len(fracs))

	for _, fraction := range fracs {
		wp.workerTasks <- Task{
			Ctx:    ctx,
			Frac:   fraction,
			Params: params,
			Resp:   respChan,
		}
	}

	qprs := make([]*seq.QPR, 0, len(fracs))
	stats := make([]*Stats, 0, len(fracs))
	var errs []error

	for i := 0; i < len(fracs); i++ {
		resp := <-respChan
		err := resp.err

		if err != nil {
			if errors.Is(err, consts.ErrTooManyUniqValues) {
				return nil, nil, err
			}

			errs = append(errs, fmt.Errorf("error during searching frac: %w", resp.err))
			continue
		}

		if resp.qpr == nil { // it is possible for a suicided fraction for example
			metric.CountersTotal.WithLabelValues("empty_qpr").Inc()
			continue
		}

		qprs = append(qprs, resp.qpr)
		stats = append(stats, resp.stats)
	}

	return qprs, stats, util.CollapseErrors(errs)
}

func work(workerTasks chan Task) {
	for task := range workerTasks {
		task.Resp <- searchFrac(task)
	}
}

func searchFrac(task Task) (resp fracResponse) {
	defer func() {
		resp.frac = task.Frac // link resp with task.Frac (for proper error handling)

		if e := util.RecoverToError(recover(), metric.StorePanics); e != nil {
			resp.err = fmt.Errorf("search panicked: frac=%s, error=%w", resp.frac.Info().Name(), e)
		}
	}()

	if util.IsCancelled(task.Ctx) {
		return fracResponse{err: task.Ctx.Err()}
	}

	// The index of the active fraction changes in parts and at a single moment in time may not be consistent.
	// So we can add new IDs to the index but update the range [from; to] with a delay.
	// Because of this, at the Search stage, we can get IDs that are outside the fraction range [from; to].
	//
	// Because of this, at the next Fetch stage, we may not find documents with such IDs, because we will ignore
	// the fraction whose range [from; to] does not contain this ID.
	//
	// To prevent this from happening, so that the Search stage and the Fetch stage work consistently,
	// we must limit the query range in accordance with the current fraction range [from; to].
	info := task.Frac.Info()
	task.Params.From = max(task.Params.From, info.From)
	task.Params.To = min(task.Params.To, info.To)

	dataProvider, release, ok := task.Frac.DataProvider(task.Ctx)
	if !ok {
		metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
		return fracResponse{}
	}

	defer release()

	if util.IsCancelled(task.Ctx) {
		return fracResponse{err: task.Ctx.Err()}
	}

	// done preparing, call actual search
	return searchFracImpl(dataProvider, task)
}

func getLIDsBorders(minMID, maxMID seq.MID, ids frac.IDsProvider) (uint32, uint32) {
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

func searchFracImpl(dataProvider frac.DataProvider, task Task) fracResponse {
	stats := NewStats(dataProvider.Type())

	sw := dataProvider.Stopwatch()

	hasAgg := task.Params.HasAgg()
	hasHist := task.Params.HasHist()

	defer sw.Export(chooseMetric(stats.FracType, hasAgg, hasHist))

	totalMetric := sw.Start("total")
	defer totalMetric.Stop()

	m := sw.Start("get_lids_borders")
	idsProvider := dataProvider.IDsProvider()
	minLID, maxLID := getLIDsBorders(task.Params.From, task.Params.To, idsProvider)
	m.Stop()

	m = sw.Start("eval_leaf")
	evalTree, err := buildEvalTree(task.Params.AST, minLID, maxLID, stats, task.Params.Order.IsReverse(),
		func(token parser.Token, stats *Stats) (node.Node, error) {
			return evalLeaf(dataProvider, token, stats, minLID, maxLID, task.Params.Order)
		},
	)
	m.Stop()

	if err != nil {
		return fracResponse{err: err}
	}

	defer func(start time.Time) {
		stats.TreeDuration = time.Since(start)
	}(time.Now())

	if util.IsCancelled(task.Ctx) {
		return fracResponse{err: task.Ctx.Err()}
	}

	aggs := make([]Aggregator, len(task.Params.AggQ))
	if hasAgg {
		m = sw.Start("eval_agg")
		for i, query := range task.Params.AggQ {
			aggs[i], err = evalAgg(dataProvider, query, stats, minLID, maxLID, task.Params.AggLimits, task.Params.Order)
			if err != nil {
				m.Stop()
				return fracResponse{err: err}
			}
		}
		m.Stop()
	}

	m = sw.Start("iterate_eval_tree")
	total, ids, histogram, err := iterateEvalTree(task, evalTree, aggs, idsProvider, sw)
	m.Stop()

	if err != nil {
		return fracResponse{err: err}
	}

	stats.HitsTotal = total

	var aggsResult []seq.QPRHistogram
	if len(task.Params.AggQ) > 0 {
		aggsResult = make([]seq.QPRHistogram, len(aggs))
		m = sw.Start("agg_node_make_map")
		for i := range aggs {
			aggsResult[i], err = aggs[i].Aggregate()
			if err != nil {
				m.Stop()
				return fracResponse{err: err}
			}

			if len(aggsResult[i].HistogramByToken) > task.Params.AggLimits.MaxGroupTokens && task.Params.AggLimits.MaxGroupTokens > 0 {
				return fracResponse{err: consts.ErrTooManyUniqValues}
			}
		}
		m.Stop()
	}

	if !task.Params.WithTotal {
		total = 0
	}

	qpr := &seq.QPR{
		IDs:       ids,
		Aggs:      aggsResult,
		Total:     uint64(total),
		Histogram: histogram,
	}

	return fracResponse{qpr: qpr, stats: stats}
}

func chooseMetric(fracType string, hasAgg, hasHist bool) *prometheus.HistogramVec {
	if fracType == frac.TypeActive {
		if hasAgg {
			return metric.ActiveAggSearchSec
		}
		if hasHist {
			return metric.ActiveHistSearchSec
		}
		return metric.ActiveRegSearchSec
	}
	if hasAgg {
		return metric.SealedAggSearchSec
	}
	if hasHist {
		return metric.SealedHistSearchSec
	}
	return metric.SealedRegSearchSec
}

func iterateEvalTree(
	task Task,
	evalTree node.Node,
	aggs []Aggregator,
	idsProvider frac.IDsProvider,
	sw *stopwatch.Stopwatch,
) (int, seq.IDSources, map[seq.MID]uint64, error) {
	fracName := task.Frac.Info().Name()
	hasHist := task.Params.HasHist()
	needScanAllRange := task.Params.IsScanAllRequest()

	var histogram map[seq.MID]uint64
	if hasHist {
		histogram = make(map[seq.MID]uint64)
	}

	total := 0
	ids := seq.IDSources{}
	var lastID seq.ID

	for {

		if util.IsCancelled(task.Ctx) {
			return total, ids, histogram, task.Ctx.Err()
		}

		needMore := len(ids) < task.Params.Limit
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
				bucket -= bucket % seq.MID(task.Params.HistInterval)
				histogram[bucket]++
			}

			if needMore {
				m = sw.Start("get_rid")
				rid := idsProvider.GetRID(seq.LID(lid))
				m.Stop()

				id := seq.ID{MID: mid, RID: rid}

				if total == 0 || lastID != id { // lids increase monotonically, it's enough to compare current id with the last one
					foundID := seq.IDSource{
						ID:     id,
						Source: 0,
						Hint:   fracName,
					}
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
