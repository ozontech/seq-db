package fracmanager

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type testFakeFrac struct {
	info          *frac.Info
	qpr           *seq.QPR
	searchesCount int
	fetchCount    int
	documents     map[seq.ID][]byte
	fetchError    error
}

func (f *testFakeFrac) Info() *frac.Info {
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

func (f *testFakeFrac) Fetch(_ context.Context, ids []seq.ID, _ bool) ([][]byte, error) {
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

func (f *testFakeFrac) FindLIDs(context.Context, []seq.ID) ([]seq.LID, error) {
	return []seq.LID{}, nil
}

func newFakeFrac(from, to seq.MID, qpr *seq.QPR) *testFakeFrac {
	return &testFakeFrac{
		info:      &frac.Info{From: from, To: to, DocsTotal: 1},
		qpr:       qpr,
		documents: make(map[seq.ID][]byte),
	}
}

func newFakeFracWithDocs(from, to seq.MID, documents map[seq.ID][]byte) *testFakeFrac {
	return &testFakeFrac{
		info:      &frac.Info{From: from, To: to, DocsTotal: uint32(len(documents))},
		documents: documents,
	}
}

func newFakeFracWithFetchError(from, to seq.MID, fetchError error) *testFakeFrac {
	return &testFakeFrac{
		info:       &frac.Info{From: from, To: to, DocsTotal: 1},
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
	_, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac2, frac1, frac3}, params, nil)
	assert.ErrorContains(t, err, "too many fractions hit", "unexpected error")

	params = processor.SearchParams{From: 101, To: 300, Limit: 100}
	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac2, frac1, frac3}, params, nil)
	assert.NoError(t, err)
	assertQPR(t, []int{200, 150, 100, 60}, qpr.IDs)
}

func TestSearcher_ShouldSkipOutOfRangeFractions(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(150, 160, newFakeQPR(155))
	frac2 := newFakeFrac(0, 99, newFakeQPR(50))
	frac3 := newFakeFrac(201, 250, newFakeQPR(220))

	params := processor.SearchParams{From: 100, To: 200, Limit: 100}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac2, frac1, frac3}, params, nil)

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

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2}, params, nil)

	assert.NoError(t, err)
	assertQPR(t, []int{5, 4, 3, 2, 1}, qpr.IDs)
}

func TestSearcher_MergeQPRWithIDs_OverlappingFracs(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(0, 100, newFakeQPR(90, 50, 30))
	frac2 := newFakeFrac(10, 110, newFakeQPR(60, 10))
	frac3 := newFakeFrac(20, 120, newFakeQPR(70, 40, 20))
	params := processor.SearchParams{From: 0, To: 150, Limit: 5, Order: seq.DocsOrderDesc}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3}, params, nil)

	assert.NoError(t, err)
	assertQPR(t, []int{90, 70, 60, 50, 40}, qpr.IDs)
}

func TestSearcher_WithLimitAndTotal(t *testing.T) {
	searcher := NewSearcher(4, SearcherCfg{})
	frac1 := newFakeFrac(0, 10, newFakeQPRwithTotal([]int{3, 1}, 2))
	frac2 := newFakeFrac(0, 10, newFakeQPRwithTotal([]int{4, 2}, 2))

	params := processor.SearchParams{From: 0, To: 100, Limit: 3}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2}, params, nil)

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

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3}, params, nil)

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

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3}, params, nil)

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

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2, frac3, frac4}, params, nil)

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
		fakeAggRow{token: "gateway", total: 5, sum: 70, min: 1, max: 30},
		fakeAggRow{token: "proxy", total: 1, sum: 5, min: 5, max: 5},
	))
	frac2 := newFakeFrac(0, 50, newFakeQPRWithAgg(
		fakeAggRow{token: "gateway", total: 2, sum: 37, min: 2, max: 35},
	))

	params := processor.SearchParams{
		From:  0,
		To:    200,
		Limit: 100,
		AggQ: []processor.AggQuery{
			{Field: &parser.Literal{Field: "service"}, Func: seq.AggFuncSum},
		},
	}

	qpr, err := searcher.SearchDocs(context.Background(), []frac.Fraction{frac1, frac2}, params, nil)

	assert.NoError(t, err)
	assert.Len(t, qpr.Aggs, 1)
	assert.Equal(t, 2, qpr.Aggs[0].Count())

	agg := qpr.Aggs[0]
	assert.Equal(t, int64(7), agg.Values.Totals.At(0))
	assert.Equal(t, float64(1), agg.Values.Mins.At(0))
	assert.Equal(t, float64(35), agg.Values.Maxs.At(0))
	assert.Equal(t, float64(107), agg.Values.Sums.At(0))
	assert.Equal(t, int64(1), agg.Values.Totals.At(1))
	assert.Equal(t, float64(5), agg.Values.Mins.At(1))
	assert.Equal(t, float64(5), agg.Values.Maxs.At(1))
	assert.Equal(t, float64(5), agg.Values.Sums.At(1))
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

	qpr, err := searcher.SearchDocs(ctx, List{&testFakeFrac{}}, params, nil)
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

type fakeAggRow struct {
	token string
	total int64
	sum   float64
	min   float64
	max   float64
}

func newFakeQPRWithAgg(rows ...fakeAggRow) *seq.QPR {
	agg := seq.NewAggregatableSamples(seq.AggFuncSum, 1, len(rows))
	for _, row := range rows {
		agg.Tokens[0].AppendString(row.token)
		agg.Values.Totals.Append(row.total)
		agg.Values.NotExists.Append(0)
		agg.Values.Sums.Append(row.sum)
		agg.Values.Mins.Append(row.min)
		agg.Values.Maxs.Append(row.max)
	}
	return &seq.QPR{
		Aggs: []seq.AggregatableSamples{*agg},
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
