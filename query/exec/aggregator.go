package exec

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

type ExecutorState byte

const (
	ExecutorStateReadingInput ExecutorState = iota
	ExecutorStateProcessingData
	ExecutorStateProducingOutput
	ExecutorStateDone
)

// aggKey identifies a single timeseries bin: the grouping token plus the
// (floored) timestamp. ts is 0 (DummyMID) for non-timeseries aggregations, so
// all samples for the same token collapse into one bucket.
type aggKey struct {
	token string
	ts    uint64
}

type DistributedAggregator struct {
	state  ExecutorState
	inputs []query.RecordProducer

	mu sync.Mutex

	// expr holds the per-func specifics. a list of expressions over the
	// same buckets will enable multi-agg later.
	expr AggExpr

	colGroupKey query.Column[string]
	colTs       *query.Column[uint64] // nil for non-timeseries aggregations

	outSchema *query.Schema

	buckets    map[aggKey]AggAcc
	sortingBuf []*query.Record

	curIdx int
}

func NewDistributedAggregator(
	inputs []query.RecordProducer,
	in *query.Schema,
	aggFunc seq.AggFunc,
	quantiles []float64,
) *DistributedAggregator {
	colTs := query.OptionalColumn[uint64](in, "ts")
	return &DistributedAggregator{
		inputs:      inputs,
		expr:        newAggExpr(aggFunc, in, quantiles),
		colGroupKey: in.MustColumn[string]("token"),
		colTs:       colTs,
		outSchema:   query.AggResultSchemaFor(aggFunc, colTs != nil),
		buckets:     make(map[aggKey]AggAcc),
		sortingBuf:  make([]*query.Record, 0),
	}
}

func (a *DistributedAggregator) Next() *query.Record {
	if a.state == ExecutorStateReadingInput {
		var wg sync.WaitGroup
		for _, input := range a.inputs {
			wg.Go(func() {
				a.drainInput(input)
			})
		}
		wg.Wait()

		a.state = ExecutorStateProcessingData
	}

	if a.state == ExecutorStateProcessingData {
		for key, acc := range a.buckets {
			a.sortingBuf = append(a.sortingBuf, a.makeRecord(key, acc))
		}

		a.sortBuckets()
		a.state = ExecutorStateProducingOutput
	}

	if a.curIdx >= len(a.sortingBuf) {
		a.state = ExecutorStateDone
		return nil
	}

	r := a.sortingBuf[a.curIdx]
	a.curIdx++

	return r
}

func (a *DistributedAggregator) makeRecord(key aggKey, acc AggAcc) *query.Record {
	vals := make([]*query.RecordVals, 0, a.outSchema.Len())
	for _, col := range a.outSchema.Cols() {
		vals = append(vals, a.resultVals(col.Name, key, acc, acc.NotExists()))
	}
	return query.NewRecord(vals)
}

func (a *DistributedAggregator) resultVals(name string, key aggKey, acc AggAcc, notExists uint64) *query.RecordVals {
	switch name {
	case "token":
		return query.NewStringVals(key.token)
	case "value":
		return query.NewFloat64Vals(acc.Value())
	case "ts":
		return query.NewUint64Vals(key.ts)
	case "quantiles":
		return query.NewFloat64ArrayVals(acc.Quantiles())
	case "not_exists":
		return query.NewUint64Vals(notExists)
	default:
		panic(fmt.Errorf("BUG: unknown aggregation result column %q", name))
	}
}

func (a *DistributedAggregator) drainInput(input query.RecordProducer) {
	for {
		r := input.Next()
		if r == nil {
			break
		}

		key := aggKey{
			token: a.colGroupKey.Val(r),
		}
		if a.colTs != nil {
			key.ts = a.colTs.Val(r)
		}

		a.mu.Lock()

		acc, exists := a.buckets[key]
		if !exists {
			acc = a.expr.NewAcc()
			a.buckets[key] = acc
		}
		acc.Merge(r)

		a.mu.Unlock()
	}
}

func (a *DistributedAggregator) Finalize() *query.Summary {
	var total, notExists uint64
	var firstErr error
	for _, i := range a.inputs {
		s := i.Finalize()
		if s == nil {
			continue
		}
		total += s.Total
		notExists += s.NotExists
		if firstErr == nil && s.Err != nil {
			firstErr = s.Err
		}
	}
	return &query.Summary{Total: total, NotExists: notExists, Err: firstErr}
}

func (a *DistributedAggregator) sortBuckets() {
	token := a.outSchema.MustColumn[string]("token")
	value := a.outSchema.MustColumn[float64]("value")
	ts := query.OptionalColumn[uint64](a.outSchema, "ts")

	// ts is the primary key (ASC), matching seq/qpr.go sortBuckets
	// where MID comes first. Within the same ts buckets are ordered by value.
	valueCmp := func(left, right *query.Record) int {
		return cmp.Compare(value.Val(right), value.Val(left))
	}
	if a.expr.SortAsc() {
		valueCmp = func(left, right *query.Record) int {
			return cmp.Compare(value.Val(left), value.Val(right))
		}
	}

	tsCmp := func(left, right *query.Record) int { return 0 }
	if ts != nil {
		tsCmp = func(left, right *query.Record) int {
			return cmp.Compare(ts.Val(left), ts.Val(right))
		}
	}

	slices.SortFunc(a.sortingBuf, func(left, right *query.Record) int {
		return cmp.Or(
			tsCmp(left, right),
			valueCmp(left, right),
			cmp.Compare(token.Val(left), token.Val(right)),
		)
	})
}
