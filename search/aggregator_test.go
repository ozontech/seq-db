package search

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

func TestSingleSourceCountAggregator(t *testing.T) {
	searchDocs := []uint32{2, 3, 5, 8, 10, 12, 15}
	sources := [][]uint32{
		{2, 3, 5, 8, 10, 12},
		{1, 4, 6, 9, 11, 13},
		{1, 2, 4, 5, 8, 11, 12},
	}

	source := node.BuildORTreeAgg(node.MakeStaticNodes(sources))
	iter := NewSourcedNodeIterator(source, nil, nil, 0, false)
	agg := NewSingleSourceCountAggregator(iter)
	for _, id := range searchDocs {
		if err := agg.Next(id); err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, map[uint32]int64{0: 6}, agg.countBySource)
	assert.Equal(t, int64(1), agg.notExists)
}

func Generate(n int) ([]uint32, uint32) {
	v := make([]uint32, n)
	last := uint32(1)
	for i := 0; i < len(v); i++ {
		v[i] = last
		last += uint32(1 + rand.Intn(5))
	}
	return v, last
}

func BenchmarkAggDeep(b *testing.B) {
	v, _ := Generate(b.N)
	src := node.NewSourcedNodeWrapper(node.NewStatic(v, false), 0)
	iter := NewSourcedNodeIterator(src, nil, make([]uint32, 1), 0, false)
	n := NewSingleSourceCountAggregator(iter)
	vals, _ := Generate(b.N)
	b.ResetTimer()
	for _, v := range vals {
		if err := n.Next(v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggWide(b *testing.B) {
	v, _ := Generate(b.N)

	factor := int(math.Sqrt(float64(b.N)))
	wide := make([][]uint32, b.N/factor)
	for i := range wide {
		for j := 0; j < factor; j++ {
			wide[i] = append(wide[i], v[rand.Intn(b.N)])
		}
		sort.Slice(wide[i], func(j, k int) bool {
			return wide[i][j] < wide[i][k]
		})
	}

	source := node.BuildORTreeAgg(node.MakeStaticNodes(wide))

	iter := NewSourcedNodeIterator(source, nil, make([]uint32, len(wide)), 0, false)
	n := NewSingleSourceCountAggregator(iter)
	vals, _ := Generate(b.N)
	b.ResetTimer()
	for _, v := range vals {
		if err := n.Next(v); err != nil {
			b.Fatal(err)
		}
	}
}

type MockDataProvider struct {
	frac.DataProvider // embed to implement DataProvider interface and override only needed methods
}

func (m *MockDataProvider) GetValByTID(tid uint32) []byte {
	return []byte(strconv.Itoa(int(tid)))
}

type IDSourcePair struct {
	LID    uint32
	Source uint32
}

type MockNode struct {
	Pairs []IDSourcePair
}

// String implements node.Sourced
func (m *MockNode) String() string {
	return reflect.TypeOf(m).String()
}

func (m *MockNode) NextSourced() (uint32, uint32, bool) {
	if len(m.Pairs) == 0 {
		return 0, 0, false
	}
	first := m.Pairs[0]
	m.Pairs = m.Pairs[1:]
	return first.LID, first.Source, true
}

func TestTwoSourceAggregator(t *testing.T) {
	r := require.New(t)

	// Mock data provider and sources.
	dp := &MockDataProvider{}
	field := &MockNode{
		Pairs: []IDSourcePair{
			{LID: 1, Source: 0},
			{LID: 2, Source: 1},
		},
	}
	groupBy := &MockNode{
		Pairs: []IDSourcePair{
			{LID: 1, Source: 0},
			{LID: 2, Source: 1},
		},
	}

	fieldTIDs := []uint32{42, 73}
	groupByTIDs := []uint32{1, 2}
	groupIterator := NewSourcedNodeIterator(groupBy, dp, groupByTIDs, 0, false)
	fieldIterator := NewSourcedNodeIterator(field, dp, fieldTIDs, 0, false)
	aggregator := NewGroupAndFieldAggregator(fieldIterator, groupIterator, true)

	// Call Next for two data points.
	r.NoError(aggregator.Next(1))
	r.NoError(aggregator.Next(2))

	// Verify countBySource map.
	expectedCountBySource := map[twoSources]int64{
		{GroupBySource: 0, FieldSource: 0}: 1,
		{GroupBySource: 1, FieldSource: 1}: 1,
	}
	for source, count := range expectedCountBySource {
		r.Equal(count, aggregator.countBySource[source])
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
	dp := &MockDataProvider{}
	field := &MockNode{
		Pairs: []IDSourcePair{
			{LID: 1, Source: 0},
		},
	}

	iter := NewSourcedNodeIterator(field, dp, []uint32{0}, 0, false)
	aggregator := NewSingleSourceCountAggregator(iter)

	r.NoError(aggregator.Next(1))

	result, err := aggregator.Aggregate()

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	expectedResult := seq.QPRHistogram{
		NotExists: 0,
		HistogramByToken: map[string]*seq.AggregationHistogram{
			// "0" because DataProvider converts TID source (tid index) to string
			"0": {
				Total: 1,
			},
		},
	}
	r.Equal(len(expectedResult.HistogramByToken), len(result.HistogramByToken))
	for token, hist := range expectedResult.HistogramByToken {
		r.Equal(hist.Total, result.HistogramByToken[token].Total)
	}
}
