package exec

import (
	"fmt"
	"math"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

// AggExpr holds everything specific to a single aggregation function.
type AggExpr interface {
	// NewAcc returns a fresh per-bucket accumulator.
	NewAcc() AggAcc
	// SortAsc reports whether buckets are sorted by value ascending (min) instead of descending.
	SortAsc() bool
}

// AggAcc accumulates the state of a single bucket across shard records.
type AggAcc interface {
	Merge(r *query.Record)
	Value() float64
	Quantiles() []float64
	NotExists() uint64
}

type notExistsState struct {
	col       query.Column[uint64]
	notExists uint64
}

func (s *notExistsState) mergeNotExists(r *query.Record) {
	s.notExists += s.col.Val(r)
}

func (s *notExistsState) NotExists() uint64 {
	return s.notExists
}

// totalExpr backs count and unique.
type totalExpr struct {
	total query.Column[uint64]
}

func (e totalExpr) NewAcc() AggAcc {
	return &totalAcc{col: e.total}
}

func (e totalExpr) SortAsc() bool {
	return false
}

type totalAcc struct {
	col   query.Column[uint64]
	total uint64
}

func (a *totalAcc) Merge(r *query.Record) {
	a.total += a.col.Val(r)
}

func (a *totalAcc) Value() float64 {
	return float64(a.total)
}

func (a *totalAcc) Quantiles() []float64 {
	return nil
}

func (a *totalAcc) NotExists() uint64 {
	return 0
}

type minExpr struct {
	min query.Column[float64]
	ne  query.Column[uint64]
}

func (e minExpr) NewAcc() AggAcc {
	return &minAcc{notExistsState: notExistsState{col: e.ne}, col: e.min}
}

func (e minExpr) SortAsc() bool {
	return true
}

type minAcc struct {
	notExistsState
	col query.Column[float64]
	val float64
	has bool
}

func (a *minAcc) Merge(r *query.Record) {
	v := a.col.Val(r)
	if !math.IsNaN(v) && (!a.has || v < a.val) {
		a.val, a.has = v, true
	}
	a.mergeNotExists(r)
}

func (a *minAcc) Value() float64 {
	if !a.has {
		return math.NaN()
	}
	return a.val
}

func (a *minAcc) Quantiles() []float64 {
	return nil
}

type maxExpr struct {
	max query.Column[float64]
	ne  query.Column[uint64]
}

func (e maxExpr) NewAcc() AggAcc {
	return &maxAcc{notExistsState: notExistsState{col: e.ne}, col: e.max}
}

func (e maxExpr) SortAsc() bool {
	return false
}

type maxAcc struct {
	notExistsState
	col query.Column[float64]
	val float64
	has bool
}

func (a *maxAcc) Merge(r *query.Record) {
	v := a.col.Val(r)
	if !math.IsNaN(v) && (!a.has || v > a.val) {
		a.val, a.has = v, true
	}
	a.mergeNotExists(r)
}

func (a *maxAcc) Value() float64 {
	if !a.has {
		return math.NaN()
	}
	return a.val
}

func (a *maxAcc) Quantiles() []float64 {
	return nil
}

type sumExpr struct {
	sum   query.Column[float64]
	total query.Column[uint64]
	ne    query.Column[uint64]
}

func (e sumExpr) NewAcc() AggAcc {
	return &sumAcc{notExistsState: notExistsState{col: e.ne}, colSum: e.sum, colTotal: e.total}
}

func (e sumExpr) SortAsc() bool {
	return false
}

type sumAcc struct {
	notExistsState
	colSum   query.Column[float64]
	colTotal query.Column[uint64]
	sum      float64
	total    uint64
}

func (a *sumAcc) Merge(r *query.Record) {
	a.sum += a.colSum.Val(r)
	a.total += a.colTotal.Val(r)
	a.mergeNotExists(r)
}

func (a *sumAcc) Value() float64 {
	if a.total != 0 {
		return a.sum
	}
	return math.NaN()
}

func (a *sumAcc) Quantiles() []float64 {
	return nil
}

type avgExpr struct {
	sum   query.Column[float64]
	total query.Column[uint64]
	ne    query.Column[uint64]
}

func (e avgExpr) NewAcc() AggAcc {
	return &avgAcc{notExistsState: notExistsState{col: e.ne}, colSum: e.sum, colTotal: e.total}
}

func (e avgExpr) SortAsc() bool {
	return false
}

type avgAcc struct {
	notExistsState
	colSum   query.Column[float64]
	colTotal query.Column[uint64]
	sum      float64
	total    uint64
}

func (a *avgAcc) Merge(r *query.Record) {
	a.sum += a.colSum.Val(r)
	a.total += a.colTotal.Val(r)
	a.mergeNotExists(r)
}

func (a *avgAcc) Value() float64 {
	if a.total != 0 {
		return a.sum / float64(a.total)
	}
	return math.NaN()
}

func (a *avgAcc) Quantiles() []float64 {
	return nil
}

type quantileExpr struct {
	min       query.Column[float64]
	max       query.Column[float64]
	samples   query.Column[[]float64]
	ne        query.Column[uint64]
	quantiles []float64
}

func (e quantileExpr) NewAcc() AggAcc {
	return &quantileAcc{
		notExistsState: notExistsState{col: e.ne},
		expr:           e,
		cont:           seq.NewSamplesContainers(),
	}
}

func (e quantileExpr) SortAsc() bool {
	return false
}

type quantileAcc struct {
	notExistsState
	expr      quantileExpr
	cont      *seq.SamplesContainer
	quantiles []float64
}

func (a *quantileAcc) Merge(r *query.Record) {
	mn := a.expr.min.Val(r)
	mx := a.expr.max.Val(r)
	if !math.IsNaN(mn) {
		a.cont.Min = min(a.cont.Min, mn)
		a.cont.Max = max(a.cont.Max, mx)
	}
	for _, v := range a.expr.samples.Val(r) {
		a.cont.InsertSample(v)
	}
	a.mergeNotExists(r)
}

func (a *quantileAcc) Value() float64 {
	return a.Quantiles()[0]
}

func (a *quantileAcc) Quantiles() []float64 {
	if a.quantiles == nil {
		quantiles := make([]float64, 0, len(a.expr.quantiles))
		for _, q := range a.expr.quantiles {
			quantiles = append(quantiles, a.cont.Quantile(q))
		}
		a.quantiles = quantiles
	}
	return a.quantiles
}

type uniqueCountExpr struct {
	values query.Column[[]string]
	ne     query.Column[uint64]
}

func (e uniqueCountExpr) NewAcc() AggAcc {
	return &uniqueCountAcc{notExistsState: notExistsState{col: e.ne}, col: e.values}
}

func (e uniqueCountExpr) SortAsc() bool {
	return false
}

type uniqueCountAcc struct {
	notExistsState
	col    query.Column[[]string]
	values map[string]struct{}
}

func (a *uniqueCountAcc) Merge(r *query.Record) {
	if a.values == nil {
		a.values = make(map[string]struct{})
	}
	for _, v := range a.col.Val(r) {
		a.values[v] = struct{}{}
	}
	a.mergeNotExists(r)
}

func (a *uniqueCountAcc) Value() float64 {
	if len(a.values) == 0 {
		return math.NaN()
	}
	return float64(len(a.values))
}

func (a *uniqueCountAcc) Quantiles() []float64 {
	return nil
}

func newAggExpr(fn seq.AggFunc, in *query.Schema, quantiles []float64) AggExpr {
	switch fn {
	case seq.AggFuncCount, seq.AggFuncUnique:
		return totalExpr{
			total: in.MustColumn[uint64]("total"),
		}
	case seq.AggFuncMin:
		return minExpr{
			min: in.MustColumn[float64]("min"),
			ne:  in.MustColumn[uint64]("not_exists"),
		}
	case seq.AggFuncMax:
		return maxExpr{
			max: in.MustColumn[float64]("max"),
			ne:  in.MustColumn[uint64]("not_exists"),
		}
	case seq.AggFuncSum:
		return sumExpr{
			sum:   in.MustColumn[float64]("sum"),
			total: in.MustColumn[uint64]("total"),
			ne:    in.MustColumn[uint64]("not_exists"),
		}
	case seq.AggFuncAvg:
		return avgExpr{
			sum:   in.MustColumn[float64]("sum"),
			total: in.MustColumn[uint64]("total"),
			ne:    in.MustColumn[uint64]("not_exists"),
		}
	case seq.AggFuncQuantile:
		if len(quantiles) == 0 {
			panic("BUG: empty quantiles")
		}
		return quantileExpr{
			min:       in.MustColumn[float64]("min"),
			max:       in.MustColumn[float64]("max"),
			samples:   in.MustColumn[[]float64]("samples"),
			ne:        in.MustColumn[uint64]("not_exists"),
			quantiles: quantiles,
		}
	case seq.AggFuncUniqueCount:
		return uniqueCountExpr{
			values: in.MustColumn[[]string]("values"),
			ne:     in.MustColumn[uint64]("not_exists"),
		}
	default:
		panic(fmt.Errorf("unimplemented aggregation func %s", fn))
	}
}
