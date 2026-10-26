package seq

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
			a.Equal(result.Aggs, test.exp.Aggs)
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
			a.Equal(result.Aggs, test.exp.Aggs)
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

	return AggregatableSamples{
		SamplesByBin: bins,
		NotExists:    int64(other["_not_exists"]),
	}
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
						{
							ValuesPool: []string{"error", "info", "warn"},
							SamplesByBin: []AggBinSamples{
								aggBin("pod1", 2, 0, 1),
								aggBin("pod2", 2, 1, 2),
							},
							NotExists: 0,
						},
					},
					Total: 2,
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 200, RID: 3}},
						{ID: ID{MID: 201, RID: 4}},
					},
					Aggs: []AggregatableSamples{
						{
							ValuesPool: []string{"info", "warn", "debug"},
							SamplesByBin: []AggBinSamples{
								aggBin("pod1", 2, 0, 2),
								aggBin("pod3", 1, 1),
							},
							NotExists: 0,
						},
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
					{
						// we append values from "right" values pool to "left" values pool, thus order is defined and can be tested
						ValuesPool: []string{"error", "info", "warn", "debug"},
						SamplesByBin: []AggBinSamples{
							aggBin("pod1", 4, 0, 1, 3),
							aggBin("pod2", 2, 1, 2),
							aggBin("pod3", 1, 2),
						},
						NotExists: 0,
					},
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
						{
							ValuesPool: []string{"error", "fatal"},
							SamplesByBin: []AggBinSamples{
								aggBin("pod1", 2, 0, 1),
							},
							NotExists: 0,
						},
					},
					Total: 1,
				},
				{
					IDs: []IDSource{
						{ID: ID{MID: 200, RID: 2}},
					},
					Aggs: []AggregatableSamples{
						{
							ValuesPool: []string{"info", "debug"},
							SamplesByBin: []AggBinSamples{
								aggBin("pod2", 2, 0, 1),
							},
							NotExists: 0,
						},
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
					{
						ValuesPool: []string{"error", "fatal", "info", "debug"},
						SamplesByBin: []AggBinSamples{
							aggBin("pod1", 2, 0, 1),
							aggBin("pod2", 2, 2, 3),
						},
						NotExists: 0,
					},
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
				a.Equal(len(expAgg.SamplesByBin), len(actualAgg.SamplesByBin), "bins count does not match")
				a.True(slices.IsSortedFunc(actualAgg.SamplesByBin, func(left, right AggBinSamples) int {
					return CompareAggBin(left.Bin, right.Bin)
				}), "bins are not sorted by (token, descending MID)")

				for _, expected := range expAgg.SamplesByBin {
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
	left := AggregatableSamples{
		SamplesByBin: []AggBinSamples{
			{Bin: AggBin{Token: "a", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 1, Min: 1, Max: 1}},
			{Bin: AggBin{Token: "b", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 4, Min: 4, Max: 4}},
		},
		NotExists: 1,
	}
	right := AggregatableSamples{
		SamplesByBin: []AggBinSamples{
			{Bin: AggBin{Token: "a", MID: 1}, Samples: SamplesContainer{Total: 1, Sum: 2, Min: 2, Max: 2, Samples: []float64{2}}},
			{Bin: AggBin{Token: "b", MID: 2}, Samples: SamplesContainer{Total: 2, Sum: 6, Min: 3, Max: 3}},
		},
		NotExists: 4,
	}

	left.Merge([]AggregatableSamples{right})

	assert.Equal(t, int64(5), left.NotExists)
	assert.Equal(t, []AggBin{
		{Token: "a", MID: 2},
		{Token: "a", MID: 1},
		{Token: "b", MID: 2},
	}, []AggBin{left.SamplesByBin[0].Bin, left.SamplesByBin[1].Bin, left.SamplesByBin[2].Bin})

	a2 := left.FindBin(AggBin{Token: "a", MID: 2})
	assert.Equal(t, int64(1), a2.Total)
	assert.Equal(t, float64(1), a2.Sum)

	a1 := left.FindBin(AggBin{Token: "a", MID: 1})
	assert.Equal(t, int64(1), a1.Total)
	assert.Equal(t, float64(2), a1.Sum)
	a1.Samples[0] = 9
	assert.Equal(t, []float64{9}, right.SamplesByBin[0].Samples.Samples)

	b := left.FindBin(AggBin{Token: "b", MID: 2})
	assert.Equal(t, int64(3), b.Total)
	assert.Equal(t, float64(10), b.Sum)
	assert.Equal(t, float64(3), b.Min)
	assert.Equal(t, float64(4), b.Max)

}

func TestMergeQPRsKWay(t *testing.T) {
	dst := &QPR{
		Aggs: []AggregatableSamples{{
			ValuesPool: []string{"error"},
			SamplesByBin: []AggBinSamples{
				{Bin: AggBin{Token: "a", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 1, Min: 1, Max: 1}},
				{Bin: AggBin{Token: "c", MID: 2}, Samples: SamplesContainer{Total: 1, Sum: 4, Min: 4, Max: 4}},
			},
			NotExists: 1,
		}},
	}
	qprs := []*QPR{
		{Aggs: []AggregatableSamples{{
			ValuesPool: []string{"info"},
			SamplesByBin: []AggBinSamples{
				aggBin("b", 1, 0),
			},
			NotExists: 2,
		}}},
		{Aggs: []AggregatableSamples{{
			ValuesPool: []string{"error", "debug"},
			SamplesByBin: []AggBinSamples{
				aggBin("b", 1, 1),
				{Bin: AggBin{Token: "c", MID: 2}, Samples: SamplesContainer{Total: 2, Sum: 3, Min: 1, Max: 2}},
			},
			NotExists: 4,
		}}},
		{Aggs: []AggregatableSamples{{
			ValuesPool: []string{"error"},
			SamplesByBin: []AggBinSamples{
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
			},
		}}},
	}
	srcSamples := qprs[2].Aggs[0].SamplesByBin[0].Samples.Samples

	MergeQPRs(dst, qprs, 10, 0, DocsOrderDesc)

	agg := dst.Aggs[0]
	assert.Equal(t, []string{"error", "info", "debug"}, agg.ValuesPool)
	assert.Equal(t, int64(7), agg.NotExists)
	assert.Equal(t, []AggBin{
		{Token: "a", MID: 2},
		{Token: "a"},
		{Token: "b"},
		{Token: "c", MID: 2},
	}, []AggBin{
		agg.SamplesByBin[0].Bin,
		agg.SamplesByBin[1].Bin,
		agg.SamplesByBin[2].Bin,
		agg.SamplesByBin[3].Bin,
	})

	a := agg.FindBin(AggBin{Token: "a"})
	assert.Equal(t, int64(1), a.Total)
	assert.Equal(t, map[uint32]struct{}{0: {}}, a.Values)
	assert.Equal(t, float64(2), a.Sum)
	a.Samples[0] = 9
	assert.Equal(t, []float64{9}, srcSamples)

	a2 := agg.FindBin(AggBin{Token: "a", MID: 2})
	assert.Equal(t, int64(1), a2.Total)
	assert.Equal(t, float64(1), a2.Sum)

	b := agg.FindBin(AggBin{Token: "b"})
	assert.Equal(t, int64(2), b.Total)
	assert.Equal(t, map[uint32]struct{}{1: {}, 2: {}}, b.Values)

	c := agg.FindBin(AggBin{Token: "c", MID: 2})
	assert.Equal(t, int64(3), c.Total)
	assert.Equal(t, float64(7), c.Sum)
	assert.Equal(t, float64(1), c.Min)
	assert.Equal(t, float64(4), c.Max)
}

func TestKWayMergeBinsCapacity(t *testing.T) {
	bins := []AggBinSamples{
		{Bin: AggBin{Token: "a"}, Samples: SamplesContainer{Total: 1}},
		{Bin: AggBin{Token: "b"}, Samples: SamplesContainer{Total: 2}},
		{Bin: AggBin{Token: "c"}, Samples: SamplesContainer{Total: 3}},
	}
	cursors := make([]mergeCursor, 4)
	for i := range cursors {
		cursors[i].bins = bins
	}

	merged := kWayMergeBins(cursors)

	assert.Len(t, merged, len(bins))
	assert.Equal(t, len(bins), cap(merged))
}

func TestTwoWayMergeBinsCapacity(t *testing.T) {
	bins := []AggBinSamples{
		{Bin: AggBin{Token: "a"}, Samples: SamplesContainer{Total: 1}},
		{Bin: AggBin{Token: "b"}, Samples: SamplesContainer{Total: 2}},
		{Bin: AggBin{Token: "c"}, Samples: SamplesContainer{Total: 3}},
	}

	merged := twoWayMergeBins(mergeCursor{bins: bins}, mergeCursor{bins: bins})

	assert.Len(t, merged, len(bins))
	assert.Equal(t, len(bins), cap(merged))
}

func TestTwoWayMergeBinsRemapsValues(t *testing.T) {
	left := mergeCursor{bins: []AggBinSamples{{
		Bin:     AggBin{Token: "a"},
		Samples: SamplesContainer{Total: 1, Values: map[uint32]struct{}{0: {}}},
	}}}
	right := mergeCursor{
		bins: []AggBinSamples{{
			Bin:     AggBin{Token: "a"},
			Samples: SamplesContainer{Total: 1, Values: map[uint32]struct{}{1: {}}},
		}},
		remap: []uint32{0, 1},
	}

	merged := twoWayMergeBins(left, right)

	assert.Len(t, merged, 1)
	assert.Equal(t, map[uint32]struct{}{0: {}, 1: {}}, merged[0].Samples.Values)
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
			aggQpr.Aggs[i].SamplesByBin = nil
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
