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

var (
	aggColTokenOut = query.AggResultSchema.MustColumn[string]("token")
	aggColValueOut = query.AggResultSchema.MustColumn[float64]("value")
	aggColTsOut    = query.AggResultSchema.MustColumn[uint64]("ts")
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
	expr, err := newAggExpr(aggFunc, in, quantiles)
	if err != nil {
		panic(fmt.Errorf("BUG: can't execute newAggExpr: %v", err))
	}

	return &DistributedAggregator{
		inputs:      inputs,
		expr:        expr,
		colGroupKey: in.MustColumn[string]("token"),
		colTs:       optionalColumn[uint64](in, "ts"),
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
			// val order must match query.AggResultSchema
			a.sortingBuf = append(a.sortingBuf, query.NewRecord([]*query.RecordVals{
				query.NewStringVals(key.token),
				query.NewFloat64Vals(acc.Value()),
				query.NewUint64Vals(key.ts),
				query.NewFloat64ArrayVals(acc.Quantiles()),
			}))
		}

		sortBuckets(a.expr.SortAsc(), a.sortingBuf)
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
	var total uint64
	var firstErr error
	for _, i := range a.inputs {
		s := i.Finalize()
		if s == nil {
			continue
		}
		total += s.Total
		if firstErr == nil && s.Err != nil {
			firstErr = s.Err
		}
	}
	return &query.Summary{Total: total, Err: firstErr}
}

func optionalColumn[T any](s *query.Schema, name string) *query.Column[T] {
	c, err := s.Column[T](name)
	if err != nil {
		return nil
	}
	return &c
}

func sortBuckets(sortAsc bool, buckets []*query.Record) {
	// ts is the primary key (ASC), matching seq/qpr.go sortBuckets
	// where MID comes first. Within the same ts buckets are ordered by value.
	valueCmp := func(left, right *query.Record) int {
		return cmp.Compare(aggColValueOut.Val(right), aggColValueOut.Val(left))
	}
	if sortAsc {
		valueCmp = func(left, right *query.Record) int {
			return cmp.Compare(aggColValueOut.Val(left), aggColValueOut.Val(right))
		}
	}

	slices.SortFunc(buckets, func(left, right *query.Record) int {
		return cmp.Or(
			cmp.Compare(aggColTsOut.Val(left), aggColTsOut.Val(right)),
			valueCmp(left, right),
			cmp.Compare(aggColTokenOut.Val(left), aggColTokenOut.Val(right)),
		)
	})
}
