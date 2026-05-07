package exec

import (
	"cmp"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/ozontech/seq-db/query"
)

type FilterExpr[T any] interface {
	// TODO: maybe we need to pass Record or RecordVals here
	Eval(T) bool
}

type Filter[T any] struct {
	input query.RecordProducer

	colIdx int
	expr   FilterExpr[T]
}

func NewFilter[T any](
	input query.RecordProducer,
	colIdx int,
	expr FilterExpr[T],
) *Filter[T] {
	return &Filter[T]{
		input:  input,
		colIdx: colIdx,
		expr:   expr,
	}
}

func (f *Filter[T]) Next() (*query.Record, bool) {
	for {
		r, has := f.input.Next()
		if !has {
			return nil, false
		}

		// TODO: some comparisons don't need decoded data
		passes := f.expr.Eval(r.Vals[f.colIdx].Decoded().(T))
		if passes {
			return r, true
		}
	}
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

type DocFilter struct {
	field  string
	filter FilterExpr[string] // TODO: all types (???)
}

func NewDocFilter(
	field string,
	filter FilterExpr[string],
) *DocFilter {
	return &DocFilter{
		field:  field,
		filter: filter,
	}
}

func (e *DocFilter) Eval(root *insaneJSON.Root) bool {
	field := root.Dig(e.field)
	return e.filter.Eval(field.AsString())
}
