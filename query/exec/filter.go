package exec

import (
	"cmp"

	"github.com/ozontech/seq-db/query"
)

type FilterExpr[T any] interface {
	Eval(T) bool
}

type Filter[T any] struct {
	input query.RecordProducer
	col   query.Column[T]
	expr  FilterExpr[T]

	// withTotal requests the accurate total of records that pass the filter.
	// When true, Finalize drains the (possibly partially consumed) input to the
	// end and reports the count of passing records as Total. When false, the
	// upstream total is forwarded unchanged.
	withTotal bool
	// passed counts records emitted via Next; Finalize keeps counting while
	// draining the remaining input.
	passed uint64
}

func NewFilter[T any](
	input query.RecordProducer,
	col query.Column[T],
	expr FilterExpr[T],
	withTotal bool,
) *Filter[T] {
	return &Filter[T]{
		input:     input,
		col:       col,
		expr:      expr,
		withTotal: withTotal,
	}
}

func (f *Filter[T]) Next() *query.Record {
	for {
		r := f.input.Next()
		if r == nil {
			return nil
		}

		passes := f.expr.Eval(f.col.Val(r))
		if passes {
			f.passed++
			return r
		}
	}
}

func (f *Filter[T]) Finalize() *query.Summary {
	upstream := f.input.Finalize()
	if !f.withTotal {
		return upstream
	}

	for f.Next() != nil {
	}

	summary := &query.Summary{Total: f.passed}
	if upstream != nil {
		summary.Err = upstream.Err
	}
	return summary
}

type Eq[T comparable] struct {
	pred T
}

func NewEq[T comparable](
	pred T,
) *Eq[T] {
	return &Eq[T]{
		pred: pred,
	}
}

func (e *Eq[T]) Eval(other T) bool {
	return other == e.pred
}

type Gt[T cmp.Ordered] struct {
	pred T
}

func NewGt[T cmp.Ordered](
	pred T,
) *Gt[T] {
	return &Gt[T]{
		pred: pred,
	}
}

func (e *Gt[T]) Eval(other T) bool {
	return other > e.pred
}

type Lt[T cmp.Ordered] struct {
	pred T
}

func NewLt[T cmp.Ordered](
	pred T,
) *Lt[T] {
	return &Lt[T]{
		pred: pred,
	}
}

func (e *Lt[T]) Eval(other T) bool {
	return other < e.pred
}
