package fracmanager

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type testFakeFrac struct {
	info          *common.Info
	qpr           *seq.QPR
	searchesCount int
	fetchCount    int
	documents     map[seq.ID][]byte
	fetchError    error
}

func (f *testFakeFrac) Info() *common.Info {
	return f.info
}

func (f *testFakeFrac) IsIntersecting(from, to seq.MID) bool {
	if f.info == nil {
		return false
	}
	return !(to < f.info.From || f.info.To < from)
}

func (f *testFakeFrac) Contains(mid seq.MID) bool {
	return f.info.IsIntersecting(mid, mid)
}

func (f *testFakeFrac) Fetch(_ context.Context, ids []seq.ID) ([][]byte, error) {
	f.fetchCount++
	if f.fetchError != nil {
		return nil, f.fetchError
	}

	docs := make([][]byte, len(ids))
	for i, id := range ids {
		if doc, exists := f.documents[id]; exists {
			docs[i] = doc
		} else {
			docs[i] = nil
		}
	}
	return docs, nil
}

func (f *testFakeFrac) Search(context.Context, processor.SearchParams) (*seq.QPR, error) {
	f.searchesCount++
	return f.qpr, nil
}

func newFakeFrac(from, to seq.MID, qpr *seq.QPR) *testFakeFrac {
	return &testFakeFrac{
		info:      &common.Info{From: from, To: to, DocsTotal: 1},
		qpr:       qpr,
		documents: make(map[seq.ID][]byte),
	}
}

func newFakeFracWithDocs(from, to seq.MID, documents map[seq.ID][]byte) *testFakeFrac {
	return &testFakeFrac{
		info:      &common.Info{From: from, To: to, DocsTotal: uint32(len(documents))},
		documents: documents,
	}
}

func newFakeFracWithFetchError(from, to seq.MID, fetchError error) *testFakeFrac {
	return &testFakeFrac{
		info:       &common.Info{From: from, To: to, DocsTotal: 1},
		documents:  make(map[seq.ID][]byte),
		fetchError: fetchError,
	}
}

func TestSearcher_MaxFractionHitsExceeded(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{MaxFractionHits: 2})
	frac1 := newFakeFrac(0, 49, newFakeQPR(10))
	frac2 := newFakeFrac(50, 200, newFakeQPR(60, 100))
	frac3 := newFakeFrac(150, 250, newFakeQPR(150, 200))

	params := processor.SearchParams{From: 0, To: 300, Limit: 100}
	_, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac2, frac1, frac3}, params)
	assert.ErrorContains(t, err, "too many fractions hit", "unexpected error")

	params = processor.SearchParams{From: 101, To: 300, Limit: 100}
	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac2, frac1, frac3}, params)
	assert.NoError(t, err)
	assertQPR(t, []int{200, 150, 100, 60}, qpr.IDs)
}

func TestSearcher_ShouldSkipOutOfRangeFractions(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(150, 160, newFakeQPR(155))
	frac2 := newFakeFrac(0, 99, newFakeQPR(50))
	frac3 := newFakeFrac(201, 250, newFakeQPR(220))

	params := processor.SearchParams{From: 100, To: 200, Limit: 100}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac2, frac1, frac3}, params)

	assert.NoError(t, err)
	// fracs with MID range outside of query range must not be called
	assert.Equal(t, 1, frac1.searchesCount)
	assert.Equal(t, 0, frac2.searchesCount)
	assert.Equal(t, 0, frac3.searchesCount)
	assertQPR(t, []int{155}, qpr.IDs)
}

func TestSearcher_MergeQPRWithIDs_NonOverlappingFracs(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(0, 10, newFakeQPR(3, 2, 1))
	frac2 := newFakeFrac(11, 20, newFakeQPR(5, 4))
	params := processor.SearchParams{From: 0, To: 20, Limit: 100, Order: seq.DocsOrderDesc}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2}, params)

	assert.NoError(t, err)
	assertQPR(t, []int{5, 4, 3, 2, 1}, qpr.IDs)
}

func TestSearcher_MergeQPRWithIDs_OverlappingFracs(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(0, 100, newFakeQPR(90, 50, 30))
	frac2 := newFakeFrac(10, 110, newFakeQPR(60, 10))
	frac3 := newFakeFrac(20, 120, newFakeQPR(70, 40, 20))
	params := processor.SearchParams{From: 0, To: 150, Limit: 5, Order: seq.DocsOrderDesc}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3}, params)

	assert.NoError(t, err)
	assertQPR(t, []int{90, 70, 60, 50, 40}, qpr.IDs)
}

func TestSearcher_WithLimitAndTotal(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(0, 10, newFakeQPRwithTotal([]int{3, 1}, 2))
	frac2 := newFakeFrac(0, 10, newFakeQPRwithTotal([]int{4, 2}, 2))

	params := processor.SearchParams{From: 0, To: 100, Limit: 3}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2}, params)

	assert.NoError(t, err)
	assertQPR(t, []int{4, 3, 2}, qpr.IDs)
	assert.Equal(t, uint64(4), qpr.Total)
}

func TestSearcher_ShouldNotSearchIfLimitIsFilled(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{FractionsPerIteration: 2})

	frac1 := newFakeFrac(0, 10, newFakeQPR(9, 1))
	frac2 := newFakeFrac(11, 30, newFakeQPR(17, 15))
	frac3 := newFakeFrac(11, 30, newFakeQPR(18, 16))

	params := processor.SearchParams{From: 0, To: 100, Limit: 4}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3}, params)

	assert.NoError(t, err)
	assertQPR(t, []int{18, 17, 16, 15}, qpr.IDs)

	// Limit was 4, frac2 and frac3 have 4 docs
	// frac1 should never be queried if SearcherCfg.FractionsPerIteration <= 2
	assert.Equal(t, 0, frac1.searchesCount)
}

func TestSearcher_ShouldNotSearchIfLimitIsFilled_OrderAsc(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{FractionsPerIteration: 2})

	frac1 := newFakeFrac(0, 10, newFakeQPR(1, 9))
	frac2 := newFakeFrac(0, 20, newFakeQPR(15, 17))
	frac3 := newFakeFrac(20, 30, newFakeQPR(16, 18))

	params := processor.SearchParams{From: 0, To: 100, Limit: 4, Order: seq.DocsOrderAsc}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3}, params)

	assert.NoError(t, err)
	assertQPR(t, []int{1, 9, 15, 17}, qpr.IDs)
	assert.Equal(t, 0, frac3.searchesCount)
}

func TestSearcher_MergeHistograms(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{FractionsPerIteration: 1})
	frac1 := newFakeFrac(0, 20, newFakeQPRWithHist(
		[]int{5},
		map[seq.MID]uint64{
			10: 1,
			15: 1,
		},
	))
	frac2 := newFakeFrac(0, 49, newFakeQPRWithHist(
		[]int{10},
		map[seq.MID]uint64{
			30: 1,
			40: 1,
		},
	))
	frac3 := newFakeFrac(50, 100, newFakeQPRWithHist(
		[]int{60, 50},
		map[seq.MID]uint64{
			100: 5,
			60:  3,
		},
	))
	frac4 := newFakeFrac(100, 150, newFakeQPRWithHist(
		[]int{80, 70},
		map[seq.MID]uint64{
			100: 2,
			130: 4,
		},
	))
	params := processor.SearchParams{
		From:         21,
		To:           200,
		Limit:        4,
		HistInterval: 10,
	}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3, frac4}, params)

	assert.NoError(t, err)
	// frac1 is outside of query range, and therefore frac1 histogram doesn't contribute to the final result
	// frac2 was queried even though limit is 4, and necessary docs were found in frac3 and frac4
	expectedHist := map[seq.MID]uint64{
		100: 7,
		130: 4,
		60:  3,
		40:  1,
		30:  1,
	}
	assert.Equal(t, expectedHist, qpr.Histogram)
	// Limit was 4
	assertQPR(t, []int{80, 70, 60, 50}, qpr.IDs)
}

func TestSearcher_MergeAggregations(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})

	frac1 := newFakeFrac(0, 50, newFakeQPRWithAgg(
		map[seq.AggBin]*seq.SamplesContainer{
			{Token: "gateway"}: {Min: 1, Max: 30, Sum: 70, Total: 5},
			{Token: "proxy"}:   {Min: 5, Max: 5, Sum: 5, Total: 1},
		},
	))
	frac2 := newFakeFrac(0, 50, newFakeQPRWithAgg(
		map[seq.AggBin]*seq.SamplesContainer{
			{Token: "gateway"}: {Min: 2, Max: 35, Sum: 37, Total: 2},
		},
	))

	params := processor.SearchParams{
		From:  0,
		To:    200,
		Limit: 100,
		AggQ: []processor.AggQuery{
			{Field: &parser.Literal{Field: "service"}, Func: seq.AggFuncSum},
		},
	}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2}, params)

	assert.NoError(t, err)
	assert.Len(t, qpr.Aggs, 1)
	assert.Len(t, qpr.Aggs[0].SamplesByBin, 2)

	gatewaySamples := qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "gateway"}]
	assert.Equal(t, int64(7), gatewaySamples.Total)
	assert.Equal(t, float64(1), gatewaySamples.Min)
	assert.Equal(t, float64(35), gatewaySamples.Max)
	assert.Equal(t, float64(107), gatewaySamples.Sum)

	proxySamples := qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "proxy"}]
	assert.Equal(t, int64(1), proxySamples.Total)
	assert.Equal(t, float64(5), proxySamples.Min)
	assert.Equal(t, float64(5), proxySamples.Max)
	assert.Equal(t, float64(5), proxySamples.Sum)
}

func TestFracsLimit(t *testing.T) {
	maxFractionHits := 10
	fracsCount := maxFractionHits + 10

	testFracs := make(List, 0, fracsCount)
	for i := 0; i < fracsCount; i++ {
		testFracs = append(testFracs, newFakeFrac(seq.MID(0), seq.MID(math.MaxUint64), nil))
	}

	s := NewSearcher(1, SearcherCfg{MaxFractionHits: maxFractionHits})
	_, err := s.prepareFracs(testFracs, processor.SearchParams{})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, consts.ErrTooManyFractionsHit))
}

func TestEmptyFracs(t *testing.T) {
	searcher := NewSearcher(1, SearcherCfg{})

	ctx := context.Background()

	seqql, err := parser.ParseSeqQL("level:7", seq.TestMapping)
	assert.NoError(t, err)

	params := processor.SearchParams{
		AST:   seqql.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: 100,
	}

	qpr, err := searcher.SearchDocs(ctx, List{&testFakeFrac{}}, params)
	assert.NoError(t, err)

	assert.Empty(t, qpr.IDs)
}

func newFakeQPRwithTotal(ids []int, total uint64) *seq.QPR {
	qpr := newFakeQPR(ids...)
	qpr.Total = total
	return qpr
}

func newFakeQPR(ids ...int) *seq.QPR {
	idsWithSource := make(seq.IDSources, len(ids))
	for i, mid := range ids {
		idsWithSource[i] = seq.IDSource{ID: seq.SimpleID(int64(mid))}
	}
	return &seq.QPR{IDs: idsWithSource}
}

func newFakeQPRWithHist(ids []int, histogram map[seq.MID]uint64) *seq.QPR {
	idsWithSource := make(seq.IDSources, len(ids))
	for i, mid := range ids {
		idsWithSource[i] = seq.IDSource{ID: seq.SimpleID(int64(mid))}
	}
	return &seq.QPR{
		IDs:       idsWithSource,
		Histogram: histogram,
	}
}

func newFakeQPRWithAgg(aggData map[seq.AggBin]*seq.SamplesContainer) *seq.QPR {
	agg := seq.AggregatableSamples{
		SamplesByBin: aggData,
		NotExists:    0,
	}

	return &seq.QPR{
		Aggs: []seq.AggregatableSamples{agg},
	}
}

func assertQPR(t *testing.T, expected []int, ids seq.IDSources) {
	t.Helper()
	mids := make([]int, len(ids))
	for i, id := range ids {
		mids[i] = int(id.ID.MID)
	}
	assert.Equal(t, expected, mids)
}
