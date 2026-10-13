package exec

import (
	"cmp"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query"
)

type FilterExpr[T any] interface {
	Eval(T) bool
}

type Filter[T any] struct {
	input query.RecordProducer

	colIdx int
	expr   FilterExpr[T]

	// withTotal requests the accurate total of records that pass the filter.
	// When true, Finalize drains the (possibly partially consumed) input to the
	// end and reports the count of passing records as Total. When false, the
	// upstream total is forwarded unchanged.
	withTotal bool
	// passed counts records emitted via Next; Finalize keeps counting while
	// draining the remaining input.
	passed uint64

	// roots holds every record whose colIdx val has been decoded (and thus
	// Spawn'd an insaneJSON root). They are released back to the library pool in
	// Finalize. Record.Release is idempotent, so records that were forwarded
	// downstream (and released there too) are safe to release here as well.
	roots []*query.Record
}

func NewFilter[T any](
	input query.RecordProducer,
	colIdx int,
	expr FilterExpr[T],
	withTotal bool,
) *Filter[T] {
	return &Filter[T]{
		input:     input,
		colIdx:    colIdx,
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

		passes := f.expr.Eval(r.Vals[f.colIdx].Decoded().(T))
		// The decoded root is now cached; keep a reference so Finalize can release it.
		f.roots = append(f.roots, r)
		if passes {
			f.passed++
			return r
		}
	}
}

func (f *Filter[T]) Finalize() *query.Summary {
	upstream := f.input.Finalize()
	if !f.withTotal {
		f.releaseRoots()
		return upstream
	}

	for f.Next() != nil {
	}
	f.releaseRoots()

	summary := &query.Summary{Total: f.passed}
	if upstream != nil {
		summary.Err = upstream.Err
	}
	return summary
}

func (f *Filter[T]) releaseRoots() {
	for _, r := range f.roots {
		r.Release()
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
	filter FilterExpr[string]
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
