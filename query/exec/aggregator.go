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

type AggSamples struct {
	Min   float64
	Max   float64
	Sum   float64
	Total uint64
}

type DistributedAggregator struct {
	state  ExecutorState
	inputs []query.RecordProducer

	aggFunc seq.AggFunc

	buckets    map[aggKey]AggSamples
	sortingBuf []*query.Record

	curIdx int
}

func NewDistributedAggregator(
	inputs []query.RecordProducer,
	aggFunc seq.AggFunc,
) *DistributedAggregator {
	return &DistributedAggregator{
		inputs:     inputs,
		aggFunc:    aggFunc,
		buckets:    make(map[aggKey]AggSamples),
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
				s := a.buckets[key]

				if s.Total == 0 {
					s.Min = r.Vals[1].Decoded().(float64)
					s.Max = r.Vals[2].Decoded().(float64)
				} else {
					s.Min = min(s.Min, r.Vals[1].Decoded().(float64))
					s.Max = max(s.Max, r.Vals[2].Decoded().(float64))
				}

				s.Sum += r.Vals[3].Decoded().(float64)
				s.Total += r.Vals[4].Decoded().(uint64)

				a.buckets[key] = s
			}
		}

		a.state = ExecutorStateProcessingData
	}

	if a.state == ExecutorStateProcessingData {
		for key, bucket := range a.buckets {
			var value float64

			// TODO: support all aggregate functions
			switch a.aggFunc {
			case seq.AggFuncCount, seq.AggFuncUnique:
				value = float64(bucket.Total)
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
			default:
				panic(fmt.Errorf("unimplemented aggregation func"))
			}

			a.sortingBuf = append(a.sortingBuf, query.NewRecord([]*query.RecordVals{
				query.NewRecordVals(query.DataTypeString, []byte(key.token)),
				query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(value)),
				query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(key.ts)),
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
