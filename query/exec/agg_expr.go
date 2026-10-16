package exec

import (
	"fmt"

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
}

// totalExpr backs count and unique.
type totalExpr struct {
	total query.Column[uint64]
}

func (e totalExpr) NewAcc() AggAcc { return &totalAcc{col: e.total} }
func (e totalExpr) SortAsc() bool  { return false }

type totalAcc struct {
	col   query.Column[uint64]
	total uint64
}

func (a *totalAcc) Merge(r *query.Record) { a.total += a.col.Val(r) }
func (a *totalAcc) Value() float64        { return float64(a.total) }
func (a *totalAcc) Quantiles() []float64  { return nil }

type minExpr struct {
	min query.Column[float64]
}

func (e minExpr) NewAcc() AggAcc { return &minAcc{col: e.min} }
func (e minExpr) SortAsc() bool  { return true }

type minAcc struct {
	col query.Column[float64]
	v   float64
	has bool
}

func (a *minAcc) Merge(r *query.Record) {
	v := a.col.Val(r)
	if !a.has || v < a.v {
		a.v, a.has = v, true
	}
}
func (a *minAcc) Value() float64       { return a.v }
func (a *minAcc) Quantiles() []float64 { return nil }

type maxExpr struct {
	max query.Column[float64]
}

func (e maxExpr) NewAcc() AggAcc { return &maxAcc{col: e.max} }
func (e maxExpr) SortAsc() bool  { return false }

type maxAcc struct {
	col query.Column[float64]
	v   float64
	has bool
}

func (a *maxAcc) Merge(r *query.Record) {
	v := a.col.Val(r)
	if !a.has || v > a.v {
		a.v, a.has = v, true
	}
}
func (a *maxAcc) Value() float64       { return a.v }
func (a *maxAcc) Quantiles() []float64 { return nil }

type sumExpr struct {
	sum query.Column[float64]
}

func (e sumExpr) NewAcc() AggAcc { return &sumAcc{col: e.sum} }
func (e sumExpr) SortAsc() bool  { return false }

type sumAcc struct {
	col query.Column[float64]
	sum float64
}

func (a *sumAcc) Merge(r *query.Record) { a.sum += a.col.Val(r) }
func (a *sumAcc) Value() float64        { return a.sum }
func (a *sumAcc) Quantiles() []float64  { return nil }

type avgExpr struct {
	sum   query.Column[float64]
	total query.Column[uint64]
}

func (e avgExpr) NewAcc() AggAcc {
	return &avgAcc{colSum: e.sum, colTotal: e.total}
}
func (e avgExpr) SortAsc() bool { return false }

type avgAcc struct {
	colSum   query.Column[float64]
	colTotal query.Column[uint64]
	sum      float64
	total    uint64
}

func (a *avgAcc) Merge(r *query.Record) {
	a.sum += a.colSum.Val(r)
	a.total += a.colTotal.Val(r)
}
func (a *avgAcc) Value() float64 {
	if a.total != 0 {
		return a.sum / float64(a.total)
	}
	return 0
}
func (a *avgAcc) Quantiles() []float64 { return nil }

type quantileExpr struct {
	colMin     query.Column[float64]
	colMax     query.Column[float64]
	colSamples query.Column[[]float64]
	quantiles  []float64
}

func (e quantileExpr) NewAcc() AggAcc {
	return &quantileAcc{expr: e, cont: seq.NewSamplesContainers()}
}
func (e quantileExpr) SortAsc() bool { return false }

type quantileAcc struct {
	expr quantileExpr
	cont *seq.SamplesContainer
	has  bool
}

func (a *quantileAcc) Merge(r *query.Record) {
	mn, mx := a.expr.colMin.Val(r), a.expr.colMax.Val(r)
	if !a.has {
		a.cont.Min, a.cont.Max = mn, mx
		a.has = true
	} else {
		a.cont.Min = min(a.cont.Min, mn)
		a.cont.Max = max(a.cont.Max, mx)
	}
	for _, v := range a.expr.colSamples.Val(r) {
		a.cont.InsertSample(v)
	}
}
func (a *quantileAcc) Value() float64 { return a.Quantiles()[0] }
func (a *quantileAcc) Quantiles() []float64 {
	qs := make([]float64, 0, len(a.expr.quantiles))
	for _, q := range a.expr.quantiles {
		qs = append(qs, a.cont.Quantile(q))
	}
	return qs
}

type uniqueCountExpr struct {
	values query.Column[[]string]
}

func (e uniqueCountExpr) NewAcc() AggAcc { return &uniqueCountAcc{col: e.values} }
func (e uniqueCountExpr) SortAsc() bool  { return false }

type uniqueCountAcc struct {
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
}
func (a *uniqueCountAcc) Value() float64       { return float64(len(a.values)) }
func (a *uniqueCountAcc) Quantiles() []float64 { return nil }

// newAggExpr builds the expression for fn, resolving input columns from the schema.
func newAggExpr(fn seq.AggFunc, in *query.Schema, quantiles []float64) (AggExpr, error) {
	switch fn {
	case seq.AggFuncCount, seq.AggFuncUnique:
		total, err := in.Column[uint64]("total")
		if err != nil {
			return nil, err
		}
		return totalExpr{total}, nil
	case seq.AggFuncMin:
		col, err := in.Column[float64]("min")
		if err != nil {
			return nil, err
		}
		return minExpr{col}, nil
	case seq.AggFuncMax:
		col, err := in.Column[float64]("max")
		if err != nil {
			return nil, err
		}
		return maxExpr{col}, nil
	case seq.AggFuncSum:
		col, err := in.Column[float64]("sum")
		if err != nil {
			return nil, err
		}
		return sumExpr{col}, nil
	case seq.AggFuncAvg:
		sum, err := in.Column[float64]("sum")
		if err != nil {
			return nil, err
		}
		total, err := in.Column[uint64]("total")
		if err != nil {
			return nil, err
		}
		return avgExpr{sum, total}, nil
	case seq.AggFuncQuantile:
		if len(quantiles) == 0 {
			return nil, fmt.Errorf("BUG: empty quantiles")
		}
		cmin, err := in.Column[float64]("min")
		if err != nil {
			return nil, err
		}
		cmax, err := in.Column[float64]("max")
		if err != nil {
			return nil, err
		}
		csamples, err := in.Column[[]float64]("samples")
		if err != nil {
			return nil, err
		}
		return quantileExpr{cmin, cmax, csamples, quantiles}, nil
	case seq.AggFuncUniqueCount:
		values, err := in.Column[[]string]("values")
		if err != nil {
			return nil, err
		}
		return uniqueCountExpr{values}, nil
	default:
		return nil, fmt.Errorf("unimplemented aggregation func")
	}
}
