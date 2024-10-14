package seq

import (
	"fmt"
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
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
				Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
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
					Aggs: []QPRHistogram{
						qprHistogramFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1}),
						qprHistogramFromMap(map[string]uint64{"llog1": 1, "llog2": 1, "llog3": 2}),
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
					Aggs: []QPRHistogram{
						qprHistogramFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1}),
						qprHistogramFromMap(map[string]uint64{"llog1": 1, "llog2": 2, "llog3": 1}),
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
				Aggs: []QPRHistogram{
					qprHistogramFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2}),
					qprHistogramFromMap(map[string]uint64{"llog1": 2, "llog2": 3, "llog3": 3}),
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
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
				Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
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
				Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 1, "log2": 2, "log3": 1})},
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
					Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 2, "log2": 1, "log3": 1})},
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
				Aggs:      []QPRHistogram{qprHistogramFromMap(map[string]uint64{"log1": 3, "log2": 3, "log3": 2})},
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
				Aggs:      make([]QPRHistogram, len(test.exp.Aggs)),
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
				Aggs:      make([]QPRHistogram, len(test.exp.Aggs)),
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

func qprHistogramFromMap(other map[string]uint64) QPRHistogram {
	histByToken := make(map[string]*AggregationHistogram, len(other))
	for k, cnt := range other {
		hist := NewAggregationHistogram()
		hist.Total = int64(cnt)
		histByToken[k] = hist
	}
	return QPRHistogram{
		HistogramByToken: histByToken,
		NotExists:        int64(other["_not_exists"]),
	}
}

func BenchmarkMergeQPRs_ReusingQPR(b *testing.B) {
	totalQPRs := uint64(100)
	qprSize := uint64(100)

	qprs := make([]*QPR, totalQPRs)
	for i := uint64(0); i < totalQPRs; i++ {
		qpr := getRandomQPR(qprSize)
		qprs[i] = &qpr
	}

	b.ResetTimer()
	aggQpr := QPR{
		Histogram: make(map[MID]uint64),
		Aggs:      make([]QPRHistogram, int(totalQPRs)),
	}

	for range b.N {
		MergeQPRs(&aggQpr, qprs, 1000, 5, DocsOrderDesc)

		aggQpr.IDs = aggQpr.IDs[:0]
		aggQpr.Errors = aggQpr.Errors[:0]
		aggQpr.Total = 0

		clear(aggQpr.Histogram)

		for i := range aggQpr.Aggs {
			clear(aggQpr.Aggs[i].HistogramByToken)
			aggQpr.Aggs[i].NotExists = 0
		}
		aggQpr.Aggs = aggQpr.Aggs[:0]
	}
}

func getRandomQPR(size uint64) QPR {
	ids := make(IDSources, size)
	hists := make(map[MID]uint64)
	aggs := make([]QPRHistogram, size)
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
		aggs[i] = qprHistogramFromMap(map[string]uint64{"_not_exists": 1})
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
