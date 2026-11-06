package seq

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
)

// assertAggsEqual compares aggregation results semantically: token rows with
// MIDs, per-row values, the values pool and the not-exists counter. A plain
// struct comparison is impossible since token columns are block-compressed.
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
	bins := make([]AggBinSamples, 0, len(other))
	for k, cnt := range other {
		hist := NewSamplesContainers()
		hist.Total = int64(cnt)
		bins = append(bins, AggBinSamples{
			Bin:     AggBin{Token: k},
			Samples: *hist,
		})
	}
	SortAggBinSamples(bins)

	result := AggregatableSamplesFromBins(bins)
	result.NotExists = int64(other["_not_exists"])
	return result
}

func aggBin(token string, total int64, values ...uint32) AggBinSamples {
	var set map[uint32]struct{}
	if len(values) > 0 {
		set = make(map[uint32]struct{}, len(values))
		for _, v := range values {
			set[v] = struct{}{}
		}
	}
	return AggBinSamples{
		Bin:     AggBin{Token: token},
		Samples: SamplesContainer{Total: total, Values: set},
	}
}

func aggFromBins(bins ...AggBinSamples) AggregatableSamples {
	return AggregatableSamplesFromBins(bins)
}

func pooledAgg(pool []string, bins ...AggBinSamples) AggregatableSamples {
	result := aggFromBins(bins...)
	samples := make([]SamplesContainer, len(bins))
	for i := range bins {
		samples[i] = bins[i].Samples
	}
	result.Values = NewAggValuesFromSamples(AggFuncUniqueCount, samples)
	result.ValuesPool = pool
	return result
}

func aggRows(agg AggregatableSamples) []AggBinSamples {
	bins := make([]AggBinSamples, 0, agg.RowsCount())
	var it StringColumnIter
	if len(agg.Tokens) > 0 {
		it = agg.Tokens[0].NewIter()
	}
	for i := 0; it.Next(); i++ {
		bins = append(bins, AggBinSamples{
			Bin:     AggBin{Token: string(it.Row()), MID: agg.RowMID(i)},
			Samples: *agg.RowSamples(i),
		})
	}
	return bins
}

func aggBins(agg AggregatableSamples) []AggBin {
	bins := make([]AggBin, 0, agg.RowsCount())
	var it StringColumnIter
	if len(agg.Tokens) > 0 {
		it = agg.Tokens[0].NewIter()
	}
	for i := 0; it.Next(); i++ {
		bins = append(bins, AggBin{Token: string(it.Row()), MID: agg.RowMID(i)})
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
							aggBin("pod1", 2, 0, 1),
							aggBin("pod2", 2, 1, 2),
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
							aggBin("pod1", 2, 0, 2),
							aggBin("pod3", 1, 1),
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
						aggBin("pod1", 4, 0, 1, 3),
						aggBin("pod2", 2, 1, 2),
						aggBin("pod3", 1, 2),
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
							aggBin("pod1", 2, 0, 1),
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
							aggBin("pod2", 2, 0, 1),
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
						aggBin("pod1", 2, 0, 1),
						aggBin("pod2", 2, 2, 3),
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
				a.True(slices.IsSortedFunc(aggRows(actualAgg), func(left, right AggBinSamples) int {
					return CompareAggBin(left.Bin, right.Bin)
				}), "bins are not sorted by (descending MID, token)")

				for _, expected := range aggRows(expAgg) {
					resultHist := actualAgg.FindBin(expected.Bin)
					a.NotNil(resultHist, "actual bin does not exist")
					a.Equal(expected.Samples.Total, resultHist.Total, "total does not match for bin")
					a.Equal(len(expected.Samples.Values), len(resultHist.Values), "values do not match")
					for idx := range expected.Samples.Values {
						_, valueExists := resultHist.Values[idx]
						a.True(valueExists, "value does not exist")
					}
				}
			}
		})
	}
}

func TestMergeAggregatableSamplesOrder(t *testing.T) {
	left := AggregatableSamplesFromBins([]AggBinSamples{
		{Bin: AggBin{Token: "a", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 1, Min: 1, Max: 1}},
		{Bin: AggBin{Token: "b", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 4, Min: 4, Max: 4}},
	})
	right := AggregatableSamplesFromBins([]AggBinSamples{
		{Bin: AggBin{Token: "b", MID: 2}, Samples: SamplesContainer{Total: 2, Sum: 6, Min: 3, Max: 3}},
		{Bin: AggBin{Token: "a", MID: 1}, Samples: SamplesContainer{Total: 1, Sum: 2, Min: 2, Max: 2, Samples: []float64{2}}},
	})
	left.NotExists = 1
	right.NotExists = 4

	left.Merge([]AggregatableSamples{right})

	assert.Equal(t, int64(5), left.NotExists)
	assert.Equal(t, []AggBin{
		{Token: "a", MID: 2},
		{Token: "b", MID: 2},
		{Token: "a", MID: 1},
	}, aggBins(left))

	a2 := left.FindBin(AggBin{Token: "a", MID: 2})
	assert.Equal(t, int64(1), a2.Total)
	assert.Equal(t, float64(1), a2.Sum)

	a1 := left.FindBin(AggBin{Token: "a", MID: 1})
	assert.Equal(t, int64(1), a1.Total)
	assert.Equal(t, float64(2), a1.Sum)

	b := left.FindBin(AggBin{Token: "b", MID: 2})
	assert.Equal(t, int64(3), b.Total)
	assert.Equal(t, float64(10), b.Sum)
	assert.Equal(t, float64(3), b.Min)
	assert.Equal(t, float64(4), b.Max)

}

func TestOrdinaryAggregationKeepsMIDsNil(t *testing.T) {
	left := NewAggregatableSamplesForFunc(AggFuncCount, 1, 2)
	left.AppendRow([]string{"b"}, consts.DummyMID, SamplesContainer{Total: 2})
	left.AppendRow([]string{"a"}, consts.DummyMID, SamplesContainer{Total: 1})
	left.SortRows()

	right := NewAggregatableSamplesForFunc(AggFuncCount, 1, 1)
	right.AppendRow([]string{"a"}, consts.DummyMID, SamplesContainer{Total: 3})

	left.Merge([]AggregatableSamples{*right})

	require.Nil(t, left.MIDs)
	require.Equal(t, []int64{4, 2}, totalsValues(left.Values.Totals))
	require.Equal(t, []AggBin{
		{Token: "a"},
		{Token: "b"},
	}, aggBins(*left))
}

func TestMultiColumnAggregatableSamplesOrder(t *testing.T) {
	agg := NewAggregatableSamplesForFunc(AggFuncCount, 2, 4)
	agg.AppendRow([]string{"b", "z"}, 1, SamplesContainer{Total: 1})
	agg.AppendRow([]string{"a", "a"}, 2, SamplesContainer{Total: 2})
	agg.AppendRow([]string{"c", "a"}, consts.DummyMID, SamplesContainer{Total: 3})
	agg.AppendRow([]string{"a", "z"}, 1, SamplesContainer{Total: 4})

	agg.SortRows()

	first := agg.Tokens[0].NewIter()
	second := agg.Tokens[1].NewIter()
	var tokens []string
	for first.Next() && second.Next() {
		tokens = append(tokens, string(first.Row()), string(second.Row()))
	}
	require.Equal(t, []MID{2, 1, 1, consts.DummyMID}, agg.midRows())
	require.Equal(t, []string{"a", "a", "a", "z", "b", "z", "c", "a"}, tokens)
}

func TestMergeQPRsKWay(t *testing.T) {
	dstAgg := pooledAgg([]string{"error"},
		[]AggBinSamples{
			{Bin: AggBin{Token: "a", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 1, Min: 1, Max: 1}},
			{Bin: AggBin{Token: "c", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 4, Min: 4, Max: 4}},
		}...,
	)
	dstAgg.NotExists = 1
	dst := &QPR{Aggs: []AggregatableSamples{dstAgg}}
	qprs := []*QPR{
		{Aggs: []AggregatableSamples{pooledAgg([]string{"info"},
			aggBin("b", 1, 0),
		)}},
		{Aggs: []AggregatableSamples{pooledAgg([]string{"error", "debug"},
			[]AggBinSamples{
				{Bin: AggBin{Token: "c", MID: 2}, Samples: SamplesContainer{Total: 2, Sum: 3, Min: 1, Max: 2}},
				aggBin("b", 1, 1),
			}...,
		)}},
		{Aggs: []AggregatableSamples{pooledAgg([]string{"error"},
			[]AggBinSamples{
				{
					Bin: AggBin{Token: "a"},
					Samples: SamplesContainer{
						Total:   1,
						Sum:     2,
						Min:     2,
						Max:     2,
						Values:  map[uint32]struct{}{0: {}},
						Samples: []float64{2},
					},
				},
			}...,
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

	a := agg.FindBin(AggBin{Token: "a"})
	assert.Equal(t, int64(1), a.Total)
	assert.Equal(t, map[uint32]struct{}{0: {}}, a.Values)

	a2 := agg.FindBin(AggBin{Token: "a", MID: 2})
	assert.Equal(t, int64(1), a2.Total)

	b := agg.FindBin(AggBin{Token: "b"})
	assert.Equal(t, int64(2), b.Total)
	assert.Equal(t, map[uint32]struct{}{1: {}, 2: {}}, b.Values)

	c := agg.FindBin(AggBin{Token: "c", MID: 2})
	assert.Equal(t, int64(3), c.Total)
}

func TestMergeCompressedColumns(t *testing.T) {
	newQPR := func(codec stringColumnCodec, rows ...AggBinSamples) *QPR {
		qpr := &QPR{Aggs: []AggregatableSamples{AggregatableSamplesFromBins(rows)}}
		qpr.Aggs[0].Tokens[0] = qpr.Aggs[0].Tokens[0].compress(codec)
		qpr.Aggs[0].Values.Totals = qpr.Aggs[0].Values.Totals.compress()
		return qpr
	}

	left := newQPR(stringColumnCodecFrontZstd,
		AggBinSamples{Bin: AggBin{Token: "a"}, Samples: SamplesContainer{Total: 2}},
		AggBinSamples{Bin: AggBin{Token: "c"}, Samples: SamplesContainer{Total: 1}},
	)
	right := newQPR(stringColumnCodecZstd,
		AggBinSamples{Bin: AggBin{Token: "a"}, Samples: SamplesContainer{Total: 3}},
		AggBinSamples{Bin: AggBin{Token: "b"}, Samples: SamplesContainer{Total: 4}},
	)

	dst := &QPR{}
	MergeQPRs(dst, []*QPR{left, right}, 10, 0, DocsOrderDesc)
	require.IsType(t, &CompressedStringColumn{}, left.Aggs[0].Tokens[0])
	require.IsType(t, &CompressedStringColumn{}, right.Aggs[0].Tokens[0])
	require.IsType(t, &DeltaBitpackedInt64Column{}, left.Aggs[0].Values.Totals)
	require.IsType(t, &DeltaBitpackedInt64Column{}, right.Aggs[0].Values.Totals)
	require.IsType(t, &RawStringColumn{}, dst.Aggs[0].Tokens[0])
	require.IsType(t, &RawInt64Column{}, dst.Aggs[0].Values.Totals)

	result := dst.Aggs[0].Aggregate(AggregateArgs{Func: AggFuncCount})
	require.Len(t, result.Buckets, 3)
	require.Equal(t, "a", result.Buckets[0].Name)
	require.Equal(t, float64(5), result.Buckets[0].Value)
	require.Equal(t, "b", result.Buckets[1].Name)
	require.Equal(t, float64(4), result.Buckets[1].Value)
	require.Equal(t, "c", result.Buckets[2].Name)
	require.Equal(t, float64(1), result.Buckets[2].Value)
}

func newMergeDst(width int) *AggregatableSamples {
	return NewAggregatableSamples(width, 0)
}

func BenchmarkTwoWayMergeColumnsDisjointHighCardinality(b *testing.B) {
	const rows = 100_000

	left := mergeColumnsForBenchmark("left", rows)
	right := mergeColumnsForBenchmark("right", rows)

	b.ReportAllocs()
	for b.Loop() {
		left.init()
		right.init()
		dst := newMergeDst(len(left.aggs.Tokens))
		twoWayMergeColumns(dst, &left, &right)
	}
}

func BenchmarkKWayMergeColumnsDisjointHighCardinality(b *testing.B) {
	const rows = 100_000

	cursors := make([]mergeCursor, 5)
	for i := range cursors {
		cursors[i] = mergeColumnsForBenchmark("source-"+strconv.Itoa(i), rows)
	}

	b.ReportAllocs()
	for b.Loop() {
		for i := range cursors {
			cursors[i].init()
		}
		dst := newMergeDst(len(cursors[0].aggs.Tokens))
		kWayMergeColumns(dst, cursors)
	}
}

func BenchmarkKWayMergeColumnsOverlappingHighCardinality(b *testing.B) {
	const rows = 100_000

	agg := mergeColumnsForBenchmark("source", rows).aggs
	cursors := make([]mergeCursor, 5)
	for i := range cursors {
		cursors[i] = mergeCursor{aggs: agg}
	}

	b.ReportAllocs()
	for b.Loop() {
		for i := range cursors {
			cursors[i].init()
		}
		dst := newMergeDst(len(cursors[0].aggs.Tokens))
		kWayMergeColumns(dst, cursors)
	}
}

func mergeColumnsForBenchmark(prefix string, rows int) mergeCursor {
	agg := NewAggregatableSamples(1, rows)
	for i := range rows {
		agg.AppendRow(
			[]string{prefix + "-key-" + fmt.Sprintf("%05d", i)},
			consts.DummyMID,
			SamplesContainer{Total: 1},
		)
	}
	return mergeCursor{aggs: agg}
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
