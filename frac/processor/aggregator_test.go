package processor

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

func TestSingleSourceCountAggregator(t *testing.T) {
	// For now input for this test is incorrect since we support
	// aggregations only for `keyword` index type.
	// Will be fixed in #310.
	searchDocs := []uint32{2, 3, 5, 8, 10, 12, 15}
	sources := [][]uint32{
		{2, 3, 5, 8, 10, 12},
		{1, 4, 6, 9, 11, 13},
		{1, 2, 4, 5, 8, 11, 12},
	}

	source := node.BuildORTreeAgg(node.MakeStaticNodes(sources))
	iter := NewSourcedNodeIterator(source, nil, nil, iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})
	agg := NewSingleSourceCountAggregator(iter, provideExtractTimeFunc(nil, nil, 0))
	for _, id := range searchDocs {
		if err := agg.Next(node.NewLIDOrderDesc(id)); err != nil {
			t.Fatal(err)
		}
	}

	assert.Equal(t, map[AggBin[uint32]]int64{
		{Source: 0}: 2,
		{Source: 2}: 4,
	}, agg.countBySource)

	assert.Equal(t, int64(1), agg.notExists)
}

func TestSingleSourceCountAggregatorWithInterval(t *testing.T) {
	// For now input for this test is incorrect since we support
	// aggregations only for `keyword` index type.
	// Will be fixed in #310.
	searchDocs := []uint32{2, 3, 5, 8, 10, 12, 15}
	sources := [][]uint32{
		{2, 3, 5, 8, 10, 12},
		{1, 4, 6, 9, 11, 13},
		{1, 2, 4, 5, 8, 11, 12},
	}

	source := node.BuildORTreeAgg(node.MakeStaticNodes(sources))
	iter := NewSourcedNodeIterator(source, nil, nil, iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})

	agg := NewSingleSourceCountAggregator(iter, func(l seq.LID) seq.MID {
		return seq.MID(l) % 3
	})

	for _, id := range searchDocs {
		if err := agg.Next(node.NewLIDOrderDesc(id)); err != nil {
			t.Fatal(err)
		}
	}

	assert.Equal(t, map[AggBin[uint32]]int64{
		{Source: 0, MID: 0}: 1,
		{Source: 2, MID: 0}: 1,
		{Source: 0, MID: 1}: 1,
		{Source: 2, MID: 2}: 3,
	}, agg.countBySource)

	assert.Equal(t, int64(1), agg.notExists)
}

func Generate(n int) ([]uint32, uint32) {
	v := make([]uint32, n)
	last := uint32(1)
	for i := range v {
		v[i] = last
		last += uint32(1 + rand.Intn(5))
	}
	return v, last
}

func BenchmarkAggDeep(b *testing.B) {
	sizes := []int{1_000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			v, _ := Generate(s)
			src := node.NewSourcedNodeWrapper(node.NewStatic(v, false), 0)
			iter := NewSourcedNodeIterator(src, nil, make([]uint32, 1), iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})
			n := NewSingleSourceCountAggregator(iter, provideExtractTimeFunc(nil, nil, 0))
			vals, _ := Generate(s)

			for b.Loop() {
				for _, v := range vals {
					if err := n.Next(node.NewLIDOrderDesc(v)); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkAggWide(b *testing.B) {
	sizes := []int{1_000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			v, _ := Generate(s)

			factor := int(math.Sqrt(float64(s)))
			wide := make([][]uint32, s/factor)
			for i := range wide {
				for range factor {
					wide[i] = append(wide[i], v[rand.Intn(s)])
				}
				slices.Sort(wide[i])
			}

			source := node.BuildORTreeAgg(node.MakeStaticNodes(wide))

			iter := NewSourcedNodeIterator(source, nil, make([]uint32, len(wide)), iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})
			n := NewSingleSourceCountAggregator(iter, provideExtractTimeFunc(nil, nil, 0))
			vals, _ := Generate(s)

			for b.Loop() {
				for _, v := range vals {
					if err := n.Next(node.NewLIDOrderDesc(v)); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

type MockTokenIndex struct {
	tokenIndex // embed to implement TokenIndex interface and override only needed methods
}

func (m *MockTokenIndex) GetValByTID(tid uint32) []byte {
	return []byte(strconv.Itoa(int(tid)))
}

type IDSourcePair struct {
	LID    node.LID
	Source uint32
}

type MockNode struct {
	Pairs []IDSourcePair
}

// String implements node.Sourced
func (m *MockNode) String() string {
	return reflect.TypeOf(m).String()
}

func (m *MockNode) NextSourced() (node.LID, uint32) {
	if len(m.Pairs) == 0 {
		return node.NullLID(), 0
	}
	first := m.Pairs[0]
	m.Pairs = m.Pairs[1:]
	return first.LID, first.Source
}

func TestTwoSourceAggregator(t *testing.T) {
	r := require.New(t)

	// Mock data provider and sources.
	dp := &MockTokenIndex{}
	field := &MockNode{
		Pairs: []IDSourcePair{
			{LID: node.NewLIDOrderDesc(1), Source: 0},
			{LID: node.NewLIDOrderDesc(2), Source: 1},
		},
	}
	groupBy := &MockNode{
		Pairs: []IDSourcePair{
			{LID: node.NewLIDOrderDesc(1), Source: 0},
			{LID: node.NewLIDOrderDesc(2), Source: 1},
		},
	}

	fieldTIDs := []uint32{42, 73}
	groupByTIDs := []uint32{1, 2}
	groupIterator := NewSourcedNodeIterator(groupBy, dp, groupByTIDs, iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})
	fieldIterator := NewSourcedNodeIterator(field, dp, fieldTIDs, iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})
	limits := AggLimits{}
	aggregator := NewGroupAndFieldAggregator(
		fieldIterator, groupIterator, provideExtractTimeFunc(nil, nil, 0), true, false, limits,
	)

	// Call Next for two data points.
	r.NoError(aggregator.Next(node.NewLIDOrderDesc(1)))
	r.NoError(aggregator.Next(node.NewLIDOrderDesc(2)))

	// Verify countBySource map.
	expectedCountBySource := map[twoSources]int64{
		{GroupBySource: 0, FieldSource: 0}: 1,
		{GroupBySource: 1, FieldSource: 1}: 1,
	}

	for source, count := range expectedCountBySource {
		r.Equal(count, aggregator.countBySource[AggBin[twoSources]{
			Source: source,
		}])
	}

	agg, err := aggregator.Aggregate()
	r.NoError(err)

	wantBuckets := []seq.AggregationBucket{
		{
			Name:  "2",
			Value: 73,
		},
		{
			Name:  "1",
			Value: 42,
		},
	}

	got := agg.Aggregate(seq.AggregateArgs{
		Func: seq.AggFuncMax,
	})

	r.Equal(wantBuckets, got.Buckets)
}

func TestSingleTreeCountAggregator(t *testing.T) {
	r := require.New(t)
	dp := &MockTokenIndex{}
	field := &MockNode{
		Pairs: []IDSourcePair{
			{LID: node.NewLIDOrderDesc(1), Source: 0},
		},
	}

	iter := NewSourcedNodeIterator(field, dp, []uint32{0}, iteratorLimit{limit: 0, err: consts.ErrTooManyGroupTokens})
	aggregator := NewSingleSourceCountAggregator(iter, provideExtractTimeFunc(nil, nil, 0))

	r.NoError(aggregator.Next(node.NewLIDOrderDesc(1)))

	result, err := aggregator.Aggregate()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedResult := seq.AggregatableSamples{
		SamplesByBin: map[seq.AggBin]*seq.SamplesContainer{
			// "0" because DataProvider converts TID source (tid index) to string
			{Token: "0"}: {Total: 1},
		},
	}

	r.Equal(len(expectedResult.SamplesByBin), len(result.SamplesByBin))
	for token, hist := range expectedResult.SamplesByBin {
		r.Equal(hist.Total, result.SamplesByBin[token].Total)
	}
}

func TestAggregatorLimitExceeded(t *testing.T) {
	// For now input for this test is incorrect since we support
	// aggregations only for `keyword` index type.
	// Will be fixed in #310.
	searchDocs := []uint32{2, 3, 5, 8, 10, 12, 15}
	sources := [][]uint32{
		{2, 3, 5, 8, 10, 12},
		{1, 4, 6, 9, 11, 13},
		{1, 2, 4, 5, 8, 11, 12},
	}

	const limit = 1

	for _, expectedErr := range []error{consts.ErrTooManyGroupTokens, consts.ErrTooManyFieldTokens} {
		source := node.BuildORTreeAgg(node.MakeStaticNodes(sources))
		iter := NewSourcedNodeIterator(source, nil, nil, iteratorLimit{limit: limit, err: expectedErr})
		agg := NewSingleSourceCountAggregator(iter, provideExtractTimeFunc(nil, nil, 0))

		var limitErr error
		var limitIteration int

		for i, id := range searchDocs {
			if err := agg.Next(node.NewLIDOrderDesc(id)); err != nil {
				limitErr = err
				limitIteration = i
				break
			}
		}

		assert.Equal(t, limit, limitIteration)
		assert.ErrorIs(t, limitErr, expectedErr)
	}
}
