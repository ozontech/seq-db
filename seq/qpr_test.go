package seq

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
)

func assertAggsEqual(t *testing.T, actual, expected []AggregatableSamples) {
	t.Helper()
	require.Equal(t, len(expected), len(actual))
	for i := range actual {
		require.Equal(t, aggRows(expected[i]), aggRows(actual[i]), "agg %d rows", i)
		require.Equal(t, expected[i].ValuesPool, actual[i].ValuesPool, "agg %d values pool", i)
		require.Equal(t, expected[i].NotExists, actual[i].NotExists, "agg %d not exists", i)
	}
}

func columnRows(col Int64Column) int {
	if col == nil {
		return 0
	}
	return col.Len()
}

func totalsValues(col Int64Column) []int64 {
	if col == nil {
		return nil
	}
	values := make([]int64, 0, col.Len())
	it := col.NewIter()
	for it.Next() {
		values = append(values, it.Value())
	}
	return values
}

func midValues(col Int64Column) []MID {
	if col == nil {
		return nil
	}
	values := make([]MID, 0, col.Len())
	it := col.NewIter()
	for it.Next() {
		values = append(values, MID(it.Value()))
	}
	return values
}

type testAggRow struct {
	tokens    []string
	mid       MID
	total     int64
	notExists int64
	sum       float64
	min       float64
	max       float64
	samples   []float64
	uniqueIDs []uint32
}

func testAgg(fn AggFunc, rows ...testAggRow) AggregatableSamples {
	tokenCnt := 0
	for _, row := range rows {
		tokenCnt = max(tokenCnt, len(row.tokens))
	}
	agg := NewAggregatableSamples(fn, tokenCnt, len(rows))
	for _, row := range rows {
		for i, token := range row.tokens {
			agg.Tokens[i].AppendString(token)
		}
		agg.appendMID(row.mid)
		switch fn {
		case AggFuncCount:
			agg.Values.Totals.Append(row.total)
		case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg:
			agg.Values.Totals.Append(row.total)
			agg.Values.NotExists.Append(row.notExists)
			agg.Values.Sums.Append(row.sum)
			agg.Values.Mins.Append(row.min)
			agg.Values.Maxs.Append(row.max)
		case AggFuncQuantile:
			agg.Values.Totals.Append(row.total)
			agg.Values.NotExists.Append(row.notExists)
			agg.Values.Sums.Append(row.sum)
			agg.Values.Mins.Append(row.min)
			agg.Values.Maxs.Append(row.max)
			agg.Values.Quantiles.Append(row.samples)
		case AggFuncUniqueCount:
			agg.Values.Totals.Append(row.total)
			agg.Values.NotExists.Append(row.notExists)
			agg.Values.UniqueIDs.Append(row.uniqueIDs)
		case AggFuncUnique:
		}
	}
	return *agg
}

func aggRows(agg AggregatableSamples) []testAggRow {
	rows := make([]testAggRow, 0, agg.Count())
	iters := make([]StringColumnIter, len(agg.Tokens))
	for i := range iters {
		iters[i] = agg.Tokens[i].NewIter()
	}
	for idx := 0; idx < agg.Count(); idx++ {
		next := true
		for i := range iters {
			next = iters[i].Next() && next
		}
		if !next {
			break
		}
		item := testAggRow{mid: agg.RowMID(idx)}
		for i := range iters {
			item.tokens = append(item.tokens, string(iters[i].Value()))
		}
		switch agg.Values.Func {
		case AggFuncCount:
			item.total = agg.Values.Totals.At(idx)
		case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg:
			item.total = agg.Values.Totals.At(idx)
			item.notExists = agg.Values.NotExists.At(idx)
			switch agg.Values.Func {
			case AggFuncSum, AggFuncAvg:
				item.sum = agg.Values.Sums.At(idx)
			case AggFuncMin:
				item.min = agg.Values.Mins.At(idx)
			case AggFuncMax:
				item.max = agg.Values.Maxs.At(idx)
			}
		case AggFuncQuantile:
			item.total = agg.Values.Totals.At(idx)
			item.notExists = agg.Values.NotExists.At(idx)
			item.min = agg.Values.Mins.At(idx)
			item.max = agg.Values.Maxs.At(idx)
			item.samples = agg.Values.Quantiles.At(idx)
		case AggFuncUniqueCount:
			item.total = agg.Values.Totals.At(idx)
			item.notExists = agg.Values.NotExists.At(idx)
			item.uniqueIDs = agg.Values.UniqueIDs.At(idx)
		}
		rows = append(rows, item)
	}
	return rows
}

func TestMergeQPRs(t *testing.T) {
	testList := []struct {
		name         string
		qprs         []*QPR
		exp          *QPR
		limit        int
		histInterval MID
	}{
		{
			name:         "without repetition",
			limit:        10,
			histInterval: MID(1),
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 91, RID: 1}},
						{ID: ID{MID: 92, RID: 2}},
						{ID: ID{MID: 93, RID: 3}},
						{ID: ID{MID: 94, RID: 4}},
					},
					Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 94: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 1001",
					},
					},
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 101, RID: 11}},
						{ID: ID{MID: 102, RID: 12}},
						{ID: ID{MID: 103, RID: 13}},
						{ID: ID{MID: 104, RID: 14}},
					},
					Histogram: map[MID]uint64{101: 1, 102: 1, 103: 1, 104: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 2004",
					},
					},
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 104, RID: 14}},
					{ID: ID{MID: 103, RID: 13}},
					{ID: ID{MID: 102, RID: 12}},
					{ID: ID{MID: 101, RID: 11}},
					{ID: ID{MID: 94, RID: 4}},
					{ID: ID{MID: 93, RID: 3}},
					{ID: ID{MID: 92, RID: 2}},
					{ID: ID{MID: 91, RID: 1}},
				},
				Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 94: 1, 101: 1, 102: 1, 103: 1, 104: 1},
				Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
				Total:     8,
				Errors: []ErrorSource{
					{
						ErrStr: "problem with source 1001",
					},
					{
						ErrStr: "problem with source 2004",
					},
				},
			},
		},
		{
			name:         "without repetition multiagg",
			limit:        10,
			histInterval: MID(1),
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 91, RID: 1}},
						{ID: ID{MID: 92, RID: 2}},
						{ID: ID{MID: 93, RID: 3}},
						{ID: ID{MID: 94, RID: 4}},
					},
					Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 94: 1},
					Aggs: []AggregatableSamples{
						aggSamplesFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1}),
						aggSamplesFromMap(map[string]uint64{"llog1": 1, "llog2": 1, "llog3": 2}),
					},
					Total: 4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 1001",
					},
					},
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 101, RID: 11}},
						{ID: ID{MID: 102, RID: 12}},
						{ID: ID{MID: 103, RID: 13}},
						{ID: ID{MID: 104, RID: 14}},
					},
					Histogram: map[MID]uint64{101: 1, 102: 1, 103: 1, 104: 1},
					Aggs: []AggregatableSamples{
						aggSamplesFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1}),
						aggSamplesFromMap(map[string]uint64{"llog1": 1, "llog2": 2, "llog3": 1}),
					},
					Total: 4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 2004",
					},
					},
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 104, RID: 14}},
					{ID: ID{MID: 103, RID: 13}},
					{ID: ID{MID: 102, RID: 12}},
					{ID: ID{MID: 101, RID: 11}},
					{ID: ID{MID: 94, RID: 4}},
					{ID: ID{MID: 93, RID: 3}},
					{ID: ID{MID: 92, RID: 2}},
					{ID: ID{MID: 91, RID: 1}},
				},
				Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 94: 1, 101: 1, 102: 1, 103: 1, 104: 1},
				Aggs: []AggregatableSamples{
					aggSamplesFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2}),
					aggSamplesFromMap(map[string]uint64{"llog1": 2, "llog2": 3, "llog3": 3}),
				},
				Total: 8,
				Errors: []ErrorSource{
					{
						ErrStr: "problem with source 1001",
					},
					{
						ErrStr: "problem with source 2004",
					},
				},
			},
		},
		{
			name:         "with repetition_1",
			limit:        10,
			histInterval: MID(1),
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 91, RID: 1}},
						{ID: ID{MID: 92, RID: 2}},
						{ID: ID{MID: 93, RID: 3}},
						{ID: ID{MID: 94, RID: 4}},
					},
					Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 94: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 1001",
					},
					},
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 101, RID: 11}},
						{ID: ID{MID: 92, RID: 2}},
						{ID: ID{MID: 103, RID: 13}},
						{ID: ID{MID: 104, RID: 14}},
					},
					Histogram: map[MID]uint64{101: 1, 92: 1, 103: 1, 104: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 2004",
					},
					},
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 104, RID: 14}},
					{ID: ID{MID: 103, RID: 13}},
					{ID: ID{MID: 101, RID: 11}},
					{ID: ID{MID: 94, RID: 4}},
					{ID: ID{MID: 93, RID: 3}},
					{ID: ID{MID: 92, RID: 2}},
					{ID: ID{MID: 91, RID: 1}},
				},
				Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 94: 1, 101: 1, 103: 1, 104: 1},
				Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
				Total:     7,
				Errors: []ErrorSource{
					{
						ErrStr: "problem with source 1001",
					},
					{
						ErrStr: "problem with source 2004",
					},
				},
			},
		},
		{
			name:         "with repetition_2",
			limit:        10,
			histInterval: MID(1),
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 92, RID: 2}},
						{ID: ID{MID: 102, RID: 12}},
						{ID: ID{MID: 93, RID: 3}},
						{ID: ID{MID: 91, RID: 1}},
					},
					Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 102: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 1001",
					},
					},
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 103, RID: 13}},
						{ID: ID{MID: 91, RID: 1}},
						{ID: ID{MID: 104, RID: 14}},
						{ID: ID{MID: 102, RID: 12}},
					},
					Histogram: map[MID]uint64{91: 1, 102: 1, 103: 1, 104: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 2004",
					},
					},
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 104, RID: 14}},
					{ID: ID{MID: 103, RID: 13}},
					{ID: ID{MID: 102, RID: 12}},
					{ID: ID{MID: 93, RID: 3}},
					{ID: ID{MID: 92, RID: 2}},
					{ID: ID{MID: 91, RID: 1}},
				},
				Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 102: 1, 103: 1, 104: 1},
				Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
				Total:     6,
				Errors: []ErrorSource{
					{
						ErrStr: "problem with source 1001",
					},
					{
						ErrStr: "problem with source 2004",
					},
				},
			},
		},
		{
			name:         "with repetition_2 + limit",
			limit:        5,
			histInterval: MID(1),
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 92, RID: 2}},
						{ID: ID{MID: 102, RID: 12}},
						{ID: ID{MID: 93, RID: 3}},
						{ID: ID{MID: 91, RID: 1}},
					},
					Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 102: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 1001",
					},
					},
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 103, RID: 13}},
						{ID: ID{MID: 91, RID: 1}},
						{ID: ID{MID: 104, RID: 14}},
						{ID: ID{MID: 102, RID: 12}},
					},
					Histogram: map[MID]uint64{91: 1, 102: 1, 103: 1, 104: 1},
					Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
					Total:     4,
					Errors: []ErrorSource{{
						ErrStr: "problem with source 2004",
					},
					},
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 104, RID: 14}},
					{ID: ID{MID: 103, RID: 13}},
					{ID: ID{MID: 102, RID: 12}},
					{ID: ID{MID: 93, RID: 3}},
					{ID: ID{MID: 92, RID: 2}},
				},
				Histogram: map[MID]uint64{91: 1, 92: 1, 93: 1, 102: 1, 103: 1, 104: 1},
				Aggs:      []AggregatableSamples{aggSamplesFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
				Total:     6,
				Errors: []ErrorSource{
					{
						ErrStr: "problem with source 1001",
					},
					{
						ErrStr: "problem with source 2004",
					},
				},
			},
		},
	}

	a := assert.New(t)

	for _, test := range testList {
		t.Run(fmt.Sprintf("SingleMerge_%s", test.name), func(t *testing.T) {
			t.Parallel()
			result := &QPR{
				Histogram: make(map[MID]uint64),
				Aggs:      make([]AggregatableSamples, len(test.exp.Aggs)),
			}
			MergeQPRs(result, test.qprs, test.limit, test.histInterval, DocsOrderDesc)

			a.Equal(result.Histogram, test.exp.Histogram)
			a.Equal(result.IDs, test.exp.IDs)
			a.Equal(result.Errors, test.exp.Errors)
			a.Equal(result.Total, test.exp.Total)
			assertAggsEqual(t, result.Aggs, test.exp.Aggs)
		})
	}

	for _, test := range testList {
		t.Run(fmt.Sprintf("IterativeMerge_%s", test.name), func(t *testing.T) {
			t.Parallel()
			result := &QPR{
				Histogram: make(map[MID]uint64),
				Aggs:      make([]AggregatableSamples, len(test.exp.Aggs)),
			}
			for _, qpr := range test.qprs {
				MergeQPRs(result, []*QPR{qpr}, test.limit, test.histInterval, DocsOrderDesc)
			}
			a.Equal(result.Histogram, test.exp.Histogram)
			a.Equal(result.IDs, test.exp.IDs)
			a.Equal(result.Errors, test.exp.Errors)
			a.Equal(result.Total, test.exp.Total)
			assertAggsEqual(t, result.Aggs, test.exp.Aggs)
		})
	}
}

func TestRemoveRepetitionsAdvancedForIds(t *testing.T) {
	testList := []struct {
		name                string
		ids                 IDSources
		expIDs              IDSources
		expRepetitionsCount uint64
	}{
		{
			name: "without_repetitions",
			ids: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 93, RID: 3}},
				{ID: ID{MID: 94, RID: 4}},
			},
			expIDs: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 93, RID: 3}},
				{ID: ID{MID: 94, RID: 4}},
			},
			expRepetitionsCount: 0,
		},
		{
			name: "with_repetitions_1",
			ids: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 93, RID: 3}},
			},
			expIDs: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 93, RID: 3}},
			},
			expRepetitionsCount: 1,
		},
		{
			name: "with_repetitions_2",
			ids: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 93, RID: 3}},
				{ID: ID{MID: 94, RID: 4}},
				{ID: ID{MID: 94, RID: 4}},
				{ID: ID{MID: 94, RID: 4}},
			},
			expIDs: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 92, RID: 2}},
				{ID: ID{MID: 93, RID: 3}},
				{ID: ID{MID: 94, RID: 4}},
			},
			expRepetitionsCount: 7,
		},
	}

	a := assert.New(t)

	for _, test := range testList {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			newIDs, repetitionsCount := removeRepetitionsAdvanced(test.ids, nil, MID(0))
			a.Equal(newIDs, test.expIDs)
			a.Equal(repetitionsCount, test.expRepetitionsCount)
		})
	}
}

func TestRemoveRepetitionsAdvancedCombined(t *testing.T) {
	testList := []struct {
		name         string
		ids          IDSources
		hist         map[MID]uint64
		expIDs       IDSources
		expHist      map[MID]uint64
		histInterval MID
	}{
		{
			name:         "without_repetitions",
			histInterval: MID(3),
			ids: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 93, RID: 2}},
				{ID: ID{MID: 94, RID: 3}},
				{ID: ID{MID: 95, RID: 4}},
			},
			expIDs: []IDSource{
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 93, RID: 2}},
				{ID: ID{MID: 94, RID: 3}},
				{ID: ID{MID: 95, RID: 4}},
			},
			hist:    map[MID]uint64{90: 1, 93: 1, 96: 1, 99: 1},
			expHist: map[MID]uint64{90: 1, 93: 1, 96: 1, 99: 1},
		},
		{
			name:         "with_repetitions",
			histInterval: MID(3),
			ids: []IDSource{
				{ID: ID{MID: 90, RID: 1}},
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 93, RID: 2}},
				{ID: ID{MID: 93, RID: 2}},
				{ID: ID{MID: 94, RID: 3}},
				{ID: ID{MID: 94, RID: 3}},
				{ID: ID{MID: 95, RID: 4}},
				{ID: ID{MID: 95, RID: 4}},
			},
			expIDs: []IDSource{
				{ID: ID{MID: 90, RID: 1}},
				{ID: ID{MID: 91, RID: 1}},
				{ID: ID{MID: 93, RID: 2}},
				{ID: ID{MID: 94, RID: 3}},
				{ID: ID{MID: 95, RID: 4}},
			},
			hist:    map[MID]uint64{90: 1, 93: 4, 96: 1, 99: 1},
			expHist: map[MID]uint64{90: 0, 93: 1, 96: 1, 99: 1},
		},
	}

	a := assert.New(t)

	for _, test := range testList {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ids, _ := removeRepetitionsAdvanced(test.ids, test.hist, test.histInterval)
			a.Equal(ids, test.expIDs)
			a.Equal(test.hist, test.expHist)
		})
	}
}

func aggSamplesFromMap(other map[string]uint64) AggregatableSamples {
	tokens := make([]string, 0, len(other))
	for token := range other {
		tokens = append(tokens, token)
	}
	slices.Sort(tokens)

	rows := make([]testAggRow, 0, len(other))
	for _, token := range tokens {
		rows = append(rows, testAggRow{
			tokens: []string{token},
			total:  int64(other[token]),
		})
	}
	result := testAgg(AggFuncCount, rows...)
	result.NotExists = int64(other["_not_exists"])
	return result
}

func pooledAgg(pool []string, rows ...testAggRow) AggregatableSamples {
	result := testAgg(AggFuncUniqueCount, rows...)
	result.ValuesPool = pool
	return result
}

func uniqueRow(token string, total int64, ids ...uint32) testAggRow {
	return testAggRow{tokens: []string{token}, total: total, uniqueIDs: ids}
}

func uniqueRowAt(token string, total int64, mid MID, ids ...uint32) testAggRow {
	return testAggRow{tokens: []string{token}, total: total, mid: mid, uniqueIDs: ids}
}

func testAggRowBin(row testAggRow) AggBin {
	return AggBin{Token: row.tokens[0], MID: row.mid}
}

func aggBins(agg AggregatableSamples) []AggBin {
	bins := make([]AggBin, 0, agg.Count())
	var it StringColumnIter
	if len(agg.Tokens) > 0 {
		it = agg.Tokens[0].NewIter()
	}
	for i := 0; it.Next(); i++ {
		bins = append(bins, AggBin{Token: string(it.Value()), MID: agg.RowMID(i)})
	}
	return bins
}

func TestMergeQPRsWithValuesPool(t *testing.T) {
	testCases := []struct {
		name string
		qprs []*QPR
		exp  *QPR
	}{
		{
			name: "merge_with_values_pool_same_values",
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 100, RID: 1}},
						{ID: ID{MID: 101, RID: 2}},
					},
					Aggs: []AggregatableSamples{
						pooledAgg([]string{"error", "info", "warn"},
							uniqueRow("pod1", 2, 0, 1),
							uniqueRow("pod2", 2, 1, 2),
						),
					},
					Total: 2,
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 200, RID: 3}},
						{ID: ID{MID: 201, RID: 4}},
					},
					Aggs: []AggregatableSamples{
						pooledAgg([]string{"info", "warn", "debug"},
							uniqueRow("pod1", 2, 0, 2),
							uniqueRow("pod3", 1, 1),
						),
					},
					Total: 2,
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 201, RID: 4}},
					{ID: ID{MID: 200, RID: 3}},
					{ID: ID{MID: 101, RID: 2}},
					{ID: ID{MID: 100, RID: 1}},
				},
				Aggs: []AggregatableSamples{
					pooledAgg([]string{"error", "info", "warn", "debug"},
						uniqueRow("pod1", 4, 0, 1, 3),
						uniqueRow("pod2", 2, 1, 2),
						uniqueRow("pod3", 1, 2),
					),
				},
				Total: 4,
			},
		},
		{
			name: "merge_with_values_pool_different_values",
			qprs: []*QPR{
				{
					IDs: []IDSource{
						{ID: ID{MID: 100, RID: 1}},
					},
					Aggs: []AggregatableSamples{
						pooledAgg([]string{"error", "fatal"},
							uniqueRow("pod1", 2, 0, 1),
						),
					},
					Total: 1,
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 200, RID: 2}},
					},
					Aggs: []AggregatableSamples{
						pooledAgg([]string{"info", "debug"},
							uniqueRow("pod2", 2, 0, 1),
						),
					},
					Total: 1,
				},
			},
			exp: &QPR{
				IDs: IDSources{
					{ID: ID{MID: 200, RID: 2}},
					{ID: ID{MID: 100, RID: 1}},
				},
				Aggs: []AggregatableSamples{
					pooledAgg([]string{"error", "fatal", "info", "debug"},
						uniqueRow("pod1", 2, 0, 1),
						uniqueRow("pod2", 2, 2, 3),
					),
				},
				Total: 2,
			},
		},
	}

	a := assert.New(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := &QPR{
				Histogram: make(map[MID]uint64),
				Aggs:      make([]AggregatableSamples, len(tc.exp.Aggs)),
			}

			MergeQPRs(result, tc.qprs, 100, 0, DocsOrderDesc)

			a.Equal(tc.exp.IDs, result.IDs, "IDs do not match")
			a.Equal(tc.exp.Total, result.Total, "total does not match")
			a.Equal(len(tc.exp.Aggs), len(result.Aggs), "aggs do not match")

			for i, expAgg := range tc.exp.Aggs {
				actualAgg := result.Aggs[i]

				a.Equal(expAgg.ValuesPool, actualAgg.ValuesPool, "value pools do not match")
				a.Equal(expAgg.NotExists, actualAgg.NotExists, "NotExists do not match")
				a.Equal(columnRows(expAgg.MIDs), columnRows(actualAgg.MIDs), "bins count does not match")
				a.True(slices.IsSortedFunc(aggRows(actualAgg), func(left, right testAggRow) int {
					return CompareAggBin(testAggRowBin(left), testAggRowBin(right))
				}), "bins are not sorted by (descending MID, token)")
				a.Equal(aggRows(expAgg), aggRows(actualAgg), "rows do not match")
			}
		})
	}
}

func TestMergeAggregatableSamplesOrder(t *testing.T) {
	left := testAgg(AggFuncSum,
		testAggRow{tokens: []string{"a"}, mid: 2, total: 1, sum: 1, min: 1, max: 1},
		testAggRow{tokens: []string{"b"}, mid: 2, total: 1, sum: 4, min: 4, max: 4},
	)
	right := testAgg(AggFuncSum,
		testAggRow{tokens: []string{"b"}, mid: 2, total: 2, sum: 6, min: 3, max: 3},
		testAggRow{tokens: []string{"a"}, mid: 1, total: 1, sum: 2, min: 2, max: 2},
	)
	left.NotExists = 1
	right.NotExists = 4

	left.Merge([]AggregatableSamples{right})

	assert.Equal(t, int64(5), left.NotExists)
	assert.Equal(t, []AggBin{
		{Token: "a", MID: 2},
		{Token: "b", MID: 2},
		{Token: "a", MID: 1},
	}, aggBins(left))

	assert.Equal(t, []testAggRow{
		{tokens: []string{"a"}, mid: 2, total: 1, sum: 1},
		{tokens: []string{"b"}, mid: 2, total: 3, sum: 10},
		{tokens: []string{"a"}, mid: 1, total: 1, sum: 2},
	}, aggRows(left))
}

func TestTwoWayMergeQuantileDoesNotAliasSourceSamples(t *testing.T) {
	left := testAgg(AggFuncQuantile,
		testAggRow{tokens: []string{"a"}, total: 1, samples: []float64{1}},
		testAggRow{tokens: []string{"b"}, total: 1, samples: []float64{10}},
	)
	right := testAgg(AggFuncQuantile,
		testAggRow{tokens: []string{"a"}, total: 1, samples: []float64{2}},
	)

	left.Merge([]AggregatableSamples{right})

	require.Equal(t, []testAggRow{
		{tokens: []string{"a"}, total: 2, samples: []float64{1, 2}},
		{tokens: []string{"b"}, total: 1, samples: []float64{10}},
	}, aggRows(left))
}

func TestMergeQPRsZeroRowAggregation(t *testing.T) {
	dst := &QPR{}
	src := &QPR{
		Aggs: []AggregatableSamples{
			*NewAggregatableSamples(AggFuncCount, 1, 0),
		},
	}
	src.Aggs[0].NotExists = 7

	MergeQPRs(dst, []*QPR{src}, 10, 0, DocsOrderDesc)

	require.Len(t, dst.Aggs, 1)
	require.Len(t, dst.Aggs[0].Tokens, 1)
	require.Zero(t, dst.Aggs[0].Count())
	require.Equal(t, AggFunc(AggFuncCount), dst.Aggs[0].Values.Func)
	require.NotNil(t, dst.Aggs[0].Values.Totals)
	require.Equal(t, int64(7), dst.Aggs[0].NotExists)
}

func TestOrdinaryAggregationKeepsMIDsNil(t *testing.T) {
	left := testAgg(AggFuncCount,
		testAggRow{tokens: []string{"a"}, total: 1},
		testAggRow{tokens: []string{"b"}, total: 2},
	)
	right := testAgg(AggFuncCount,
		testAggRow{tokens: []string{"a"}, total: 3},
	)

	left.Merge([]AggregatableSamples{right})

	require.Nil(t, left.MIDs)
	require.Equal(t, []int64{4, 2}, totalsValues(left.Values.Totals))
	require.Equal(t, []AggBin{
		{Token: "a"},
		{Token: "b"},
	}, aggBins(left))
}

func TestMultiColumnAggregatableSamplesOrder(t *testing.T) {
	agg := testAgg(AggFuncCount,
		testAggRow{tokens: []string{"a", "a"}, mid: 2, total: 2},
		testAggRow{tokens: []string{"a", "z"}, mid: 1, total: 4},
		testAggRow{tokens: []string{"b", "z"}, mid: 1, total: 1},
		testAggRow{tokens: []string{"c", "a"}, mid: consts.DummyMID, total: 3},
	)

	first := agg.Tokens[0].NewIter()
	second := agg.Tokens[1].NewIter()
	var tokens []string
	for first.Next() && second.Next() {
		tokens = append(tokens, string(first.Value()), string(second.Value()))
	}
	require.Equal(t, []MID{2, 1, 1, consts.DummyMID}, midValues(agg.MIDs))
	require.Equal(t, []string{"a", "a", "a", "z", "b", "z", "c", "a"}, tokens)
}

func TestMergeQPRsKWay(t *testing.T) {
	dstAgg := pooledAgg([]string{"error"},
		uniqueRowAt("a", 1, 2),
		uniqueRowAt("c", 1, 2),
	)
	dstAgg.NotExists = 1
	dst := &QPR{Aggs: []AggregatableSamples{dstAgg}}
	qprs := []*QPR{
		{Aggs: []AggregatableSamples{pooledAgg([]string{"info"},
			uniqueRow("b", 1, 0),
		)}},
		{Aggs: []AggregatableSamples{pooledAgg([]string{"error", "debug"},
			uniqueRowAt("c", 2, 2, 0),
			uniqueRow("b", 1, 1),
		)}},
		{Aggs: []AggregatableSamples{pooledAgg([]string{"error"},
			uniqueRow("a", 1, 0),
		)}},
	}
	qprs[0].Aggs[0].NotExists = 2
	qprs[1].Aggs[0].NotExists = 4

	MergeQPRs(dst, qprs, 10, 0, DocsOrderDesc)

	agg := dst.Aggs[0]
	assert.Equal(t, []string{"error", "info", "debug"}, agg.ValuesPool)
	assert.Equal(t, int64(7), agg.NotExists)
	assert.Equal(t, []AggBin{
		{Token: "a", MID: 2},
		{Token: "c", MID: 2},
		{Token: "a"},
		{Token: "b"},
	}, aggBins(agg))

	assert.Equal(t, []testAggRow{
		{tokens: []string{"a"}, mid: 2, total: 1, uniqueIDs: []uint32{}},
		uniqueRowAt("c", 3, 2, 0),
		uniqueRow("a", 1, 0),
		uniqueRow("b", 2, 1, 2),
	}, aggRows(agg))
}

func BenchmarkMergeQPRs_ReusingQPR(b *testing.B) {
	totalQPRs := uint64(100)
	qprSize := uint64(100)

	aggQpr := QPR{
		Histogram: make(map[MID]uint64),
		Aggs:      make([]AggregatableSamples, int(totalQPRs)),
	}

	for b.Loop() {
		b.StopTimer()
		qprs := make([]*QPR, totalQPRs)
		for i := range totalQPRs {
			qpr := getRandomQPR(qprSize)
			qprs[i] = &qpr
		}
		b.StartTimer()

		MergeQPRs(&aggQpr, qprs, 1000, 5, DocsOrderDesc)

		aggQpr.IDs = aggQpr.IDs[:0]
		aggQpr.Errors = aggQpr.Errors[:0]
		aggQpr.Total = 0

		clear(aggQpr.Histogram)

		for i := range aggQpr.Aggs {
			aggQpr.Aggs[i].Tokens = nil
			aggQpr.Aggs[i].MIDs = nil
			aggQpr.Aggs[i].Values = AggValues{}
			aggQpr.Aggs[i].NotExists = 0
		}
		aggQpr.Aggs = aggQpr.Aggs[:0]
	}
}

func getRandomQPR(size uint64) QPR {
	ids := make(IDSources, size)
	hists := make(map[MID]uint64)
	aggs := make([]AggregatableSamples, size)
	errs := make([]ErrorSource, 0)

	curTime := time.Now()

	getTime := func() time.Time {
		curTime = curTime.Add(500 * time.Microsecond)
		return curTime
	}

	for i := uint64(0); i < size; i++ {
		ids[i] = IDSource{ID: NewID(getTime(), i%10), Source: i}
	}

	for i := uint64(0); i < size; i++ {
		aggs[i] = aggSamplesFromMap(map[string]uint64{"_not_exists": 1})
	}

	for i := uint64(0); i < size; i++ {
		hists[NewID(getTime(), i%10).MID]++
	}

	for i := uint64(0); i < size; i++ {
		errs = append(errs, ErrorSource{ErrStr: "error", Source: i})
	}

	return QPR{
		IDs:       ids,
		Histogram: hists,
		Aggs:      aggs,
		Total:     size,
		Errors:    errs,
	}
}
