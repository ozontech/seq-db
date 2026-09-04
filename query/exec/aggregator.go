package exec

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
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

	aggFunc   seq.AggFunc
	quantiles []float64

	buckets    map[aggKey]*seq.SamplesContainer
	sortingBuf []*query.Record

	// values holds the unique field-value sets per bucket for unique count.
	values map[aggKey]map[string]struct{}

	curIdx int
}

func NewDistributedAggregator(
	inputs []query.RecordProducer,
	aggFunc seq.AggFunc,
	quantiles []float64,
) *DistributedAggregator {
	return &DistributedAggregator{
		inputs:     inputs,
		aggFunc:    aggFunc,
		quantiles:  quantiles,
		buckets:    make(map[aggKey]*seq.SamplesContainer),
		sortingBuf: make([]*query.Record, 0),
	}
}

func (a *DistributedAggregator) Next() *query.Record {
	if a.state == ExecutorStateReadingInput {
		// TODO: read from all inputs simultaneously (???)
		for _, input := range a.inputs {
			for {
				r := input.Next()
				if r == nil {
					break
				}

				key := aggKey{
					token: r.Vals[0].Decoded().(string),
					ts:    r.Vals[6].Decoded().(uint64),
				}

				s, exists := a.buckets[key]
				if !exists {
					s = seq.NewSamplesContainers()
				}

				if !exists {
					s.Min = r.Vals[1].Decoded().(float64)
					s.Max = r.Vals[2].Decoded().(float64)
				} else {
					s.Min = min(s.Min, r.Vals[1].Decoded().(float64))
					s.Max = max(s.Max, r.Vals[2].Decoded().(float64))
				}

				s.Sum += r.Vals[3].Decoded().(float64)
				s.Total += int64(r.Vals[4].Decoded().(uint64))

				if a.aggFunc == seq.AggFuncQuantile {
					for _, v := range r.Vals[7].Decoded().([]float64) {
						s.InsertSample(v)
					}
				}

				if a.aggFunc == seq.AggFuncUniqueCount {
					if a.values == nil {
						a.values = make(map[aggKey]map[string]struct{})
					}
					m, ok := a.values[key]
					if !ok {
						m = make(map[string]struct{})
						a.values[key] = m
					}
					for _, v := range r.Vals[8].Decoded().([]string) {
						m[v] = struct{}{}
					}
				}

				a.buckets[key] = s
			}
		}

		a.state = ExecutorStateProcessingData
	}

	if a.state == ExecutorStateProcessingData {
		for key, bucket := range a.buckets {
			var value float64
			var quantiles []float64

			switch a.aggFunc {
			case seq.AggFuncCount, seq.AggFuncUnique:
				value = float64(bucket.Total)
			case seq.AggFuncUniqueCount:
				value = float64(len(a.values[key]))
			case seq.AggFuncSum:
				value = bucket.Sum
			case seq.AggFuncMin:
				value = bucket.Min
			case seq.AggFuncMax:
				value = bucket.Max
			case seq.AggFuncAvg:
				if bucket.Total != 0 {
					value = bucket.Sum / float64(bucket.Total)
				}
			case seq.AggFuncQuantile:
				if len(a.quantiles) == 0 {
					panic(fmt.Errorf("BUG: empty quantiles"))
				}
				quantiles = make([]float64, 0, len(a.quantiles))
				for _, q := range a.quantiles {
					quantiles = append(quantiles, bucket.Quantile(q))
				}
				value = quantiles[0]
			default:
				panic(fmt.Errorf("unimplemented aggregation func"))
			}

			a.sortingBuf = append(a.sortingBuf, query.NewRecord([]*query.RecordVals{
				query.NewRecordVals(query.DataTypeString, []byte(key.token)),
				query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(value)),
				query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(key.ts)),
				query.NewRecordVals(query.DataTypeFloat64Array, encoding.Float64ArrayToBytes(quantiles)),
			}))
		}

		sortBuckets(a.aggFunc, a.sortingBuf)
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

func sortBuckets(aggFunc seq.AggFunc, buckets []*query.Record) {
	// ts (Vals[2]) is the primary key (ASC), matching seq/qpr.go sortBuckets
	// where MID comes first. Within the same ts buckets are ordered by value.
	sortByTsValueDescNameAsc := func(left, right *query.Record) int {
		return cmp.Or(
			cmp.Compare(left.Vals[2].Decoded().(uint64), right.Vals[2].Decoded().(uint64)),
			cmp.Compare(right.Vals[1].Decoded().(float64), left.Vals[1].Decoded().(float64)),
			cmp.Compare(left.Vals[0].Decoded().(string), right.Vals[0].Decoded().(string)),
		)
	}

	sortByTsValueNameAsc := func(left, right *query.Record) int {
		return cmp.Or(
			cmp.Compare(left.Vals[2].Decoded().(uint64), right.Vals[2].Decoded().(uint64)),
			cmp.Compare(left.Vals[1].Decoded().(float64), right.Vals[1].Decoded().(float64)),
			cmp.Compare(left.Vals[0].Decoded().(string), right.Vals[0].Decoded().(string)),
		)
	}

	sortFunc := sortByTsValueDescNameAsc

	if aggFunc == seq.AggFuncMin {
		// Sort the MIN aggregation result in ascending order.
		sortFunc = sortByTsValueNameAsc
	}

	slices.SortFunc(buckets, sortFunc)
}
