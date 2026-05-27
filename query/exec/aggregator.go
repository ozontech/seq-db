package exec

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

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

	buckets    map[string]AggSamples
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
		buckets:    make(map[string]AggSamples),
		sortingBuf: make([]*query.Record, 0),
	}
}

func (a *DistributedAggregator) Next() (*query.Record, *query.Metadata) {
	if a.state == ExecutorStateReadingInput {
		// TODO: read from all inputs simultaneously (???)
		for _, input := range a.inputs {
			for {
				r, meta := input.Next()
				if meta != nil {
					return nil, meta
				}
				if r == nil {
					break
				}

				key := r.Vals[0].Decoded().(string)
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
		for token, bucket := range a.buckets {
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
				query.NewRecordVals(query.DataTypeString, []byte(token)),
				query.NewRecordVals(query.DataTypeFloat64, float64ToBytes(value)),
			}))
		}

		sortBuckets(a.aggFunc, a.sortingBuf)
		a.state = ExecutorStateProducingOutput
	}

	if a.curIdx >= len(a.sortingBuf) {
		a.state = ExecutorStateDone
		return nil, nil
	}

	r := a.sortingBuf[a.curIdx]
	a.curIdx++

	return r, nil
}

func sortBuckets(aggFunc seq.AggFunc, buckets []*query.Record) {
	sortByValueDescNameAsc := func(left, right *query.Record) int {
		return cmp.Or(
			cmp.Compare(right.Vals[1].Decoded().(float64), left.Vals[1].Decoded().(float64)),
			cmp.Compare(left.Vals[0].Decoded().(string), right.Vals[0].Decoded().(string)),
		)
	}

	sortByValueNameAsc := func(left, right *query.Record) int {
		return cmp.Or(
			cmp.Compare(left.Vals[1].Decoded().(float64), right.Vals[1].Decoded().(float64)),
			cmp.Compare(left.Vals[0].Decoded().(string), right.Vals[0].Decoded().(string)),
		)
	}

	sortFunc := sortByValueDescNameAsc

	if aggFunc == seq.AggFuncMin {
		// Sort the MIN aggregation result in ascending order.
		sortFunc = sortByValueNameAsc
	}

	slices.SortFunc(buckets, sortFunc)
}

func float64ToBytes(val float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(val))
	return b
}
