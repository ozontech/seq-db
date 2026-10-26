package asyncsearcher

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestQPRMarshalUnmarshal(t *testing.T) {
	test := func(qpr seq.QPR) {
		t.Helper()

		rawQPR := marshalQPR(&qpr, nil)
		var out seq.QPR
		tail, err := unmarshalQPR(&out, rawQPR, math.MaxInt)
		require.NoError(t, err)
		require.Equal(t, 0, len(tail))
		require.EqualExportedValues(t, qpr, out)
		require.Equal(t, qprBinVersion3, qprBinVersion(rawQPR[0]))
	}

	test(seq.QPR{Histogram: map[seq.MID]uint64{}})
	test(seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Errors:    []seq.ErrorSource{{ErrStr: "error", Source: 1}},
	})
	test(seq.QPR{
		IDs: seq.IDSources{
			{
				ID: seq.ID{MID: 42, RID: 13},
			},
		},
		Histogram: map[seq.MID]uint64{42: 1},
		Total:     1,
	})

	test(seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs: []seq.AggregatableSamples{
			{
				SamplesByBin: []seq.AggBinSamples{
					{Bin: seq.AggBin{Token: "_not_exists"}, Samples: seq.SamplesContainer{Total: 1}},
					{
						Bin: seq.AggBin{Token: "seq-db proxy"},
						Samples: seq.SamplesContainer{
							Min:       0,
							Max:       100,
							Sum:       100,
							Total:     1,
							NotExists: 0,
							Samples:   []float64{100},
						},
					},
					{
						Bin: seq.AggBin{Token: "seq-db store", MID: seq.MID(2)},
						Samples: seq.SamplesContainer{
							Min:       3,
							Max:       5,
							Sum:       794,
							Total:     1,
							NotExists: 7,
							Samples:   []float64{324},
						},
					},
					{
						Bin: seq.AggBin{Token: "seq-db store", MID: seq.MID(1)},
						Samples: seq.SamplesContainer{
							Min:       2,
							Max:       6,
							Sum:       544,
							Total:     2,
							NotExists: 3,
							Samples:   []float64{324},
						},
					},
				},
				NotExists: 5412,
			},
		},
	})

	test(seq.QPR{
		Histogram: map[seq.MID]uint64{},
		IDs: seq.IDSources{
			seq.IDSource{ID: seq.ID{MID: 42, RID: 13}},
		},
		Total:  545454,
		Errors: []seq.ErrorSource{{ErrStr: "context canceled", Source: 8956}},
	})

	for i := 0; i < 100; i++ {
		r := rand.N(8)
		qpr := getRandomQPR(r * 1024)
		test(qpr)
	}
}

// TestQPRVersion1Compatibility tests that it's possible to unmarshall and read version 1 async search result encoded.
// MIDs in IDs and a histogram must be converted from millis to nanos
func TestQPRVersion1Compatibility(t *testing.T) {
	qpr := seq.QPR{
		IDs: seq.IDSources{
			{
				ID: seq.ID{MID: seq.MID(1761812502573), RID: 34734732392},
			},
		},
		Histogram: map[seq.MID]uint64{
			seq.MID(1761812502573): 433,
			seq.MID(1761812502463): 743,
		},
		Aggs: []seq.AggregatableSamples{
			{
				SamplesByBin: []seq.AggBinSamples{
					{Bin: seq.AggBin{Token: "_not_exists"}, Samples: seq.SamplesContainer{Total: 1}},
					{
						Bin: seq.AggBin{Token: "seq-db store", MID: seq.MID(1761812502456)},
						Samples: seq.SamplesContainer{
							Min:       2,
							Max:       6,
							Sum:       544,
							Total:     2,
							NotExists: 3,
							Samples:   []float64{324},
						},
					},
					{
						Bin: seq.AggBin{Token: "seq-db store", MID: seq.MID(1761812502953)},
						Samples: seq.SamplesContainer{
							Min:       3,
							Max:       5,
							Sum:       794,
							Total:     1,
							NotExists: 7,
							Samples:   []float64{324},
						},
					},
				},
				NotExists: 5412,
			},
		},
	}

	rawQPR := marshalQPR(&qpr, nil)
	rawQPR[0] = uint8(qprBinVersion1)

	var outQPR seq.QPR
	tail, err := unmarshalQPR(&outQPR, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))

	require.Equal(t, seq.MID(1761812502573000000), outQPR.IDs[0].ID.MID, "mid doesn't match, should convert to nanoseconds")

	require.Len(t, outQPR.Histogram, 2)
	require.Equal(t, uint64(433), outQPR.Histogram[seq.MID(1761812502573000000)], "histogram bucket doesn't match")
	require.Equal(t, uint64(743), outQPR.Histogram[seq.MID(1761812502463000000)], "histogram bucket doesn't match")

	require.Len(t, outQPR.Aggs, 1, "should have one AggregatableSamples")
	agg := outQPR.Aggs[0]
	require.Len(t, agg.SamplesByBin, 3, "should have 3 samples in bin")

	notExistsBin := seq.AggBin{Token: "_not_exists"}
	require.Equal(t, int64(1), agg.FindBin(notExistsBin).Total, "bucket doesn't match")
	bin1 := seq.AggBin{Token: "seq-db store", MID: seq.MID(1761812502953000000)}
	require.Equal(t, int64(1), agg.FindBin(bin1).Total, "bucket doesn't match")
	bin2 := seq.AggBin{Token: "seq-db store", MID: seq.MID(1761812502456000000)}
	require.Equal(t, int64(2), agg.FindBin(bin2).Total, "bucket doesn't match")
}

func TestQPRVersion2SortsAggBins(t *testing.T) {
	sortedQPR := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs: []seq.AggregatableSamples{{
			SamplesByBin: []seq.AggBinSamples{
				{Bin: seq.AggBin{Token: "a", MID: 3}},
				{Bin: seq.AggBin{Token: "a", MID: 1}},
				{Bin: seq.AggBin{Token: "b", MID: 2}},
			},
		}},
	}

	rawQPR := marshalQPR(&sortedQPR, nil)
	var version3 seq.QPR
	tail, err := unmarshalQPR(&version3, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Equal(t, sortedQPR.Aggs[0].SamplesByBin, version3.Aggs[0].SamplesByBin)

	unsortedQPR := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs: []seq.AggregatableSamples{{
			SamplesByBin: []seq.AggBinSamples{
				{Bin: seq.AggBin{Token: "b", MID: 2}},
				{Bin: seq.AggBin{Token: "a", MID: 3}},
				{Bin: seq.AggBin{Token: "a", MID: 1}},
			},
		}},
	}
	rawQPR = slices.Clone(marshalQPR(&unsortedQPR, nil))
	rawQPR[0] = uint8(qprBinVersion2)

	var version2 seq.QPR
	tail, err = unmarshalQPR(&version2, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Equal(t, sortedQPR.Aggs[0].SamplesByBin, version2.Aggs[0].SamplesByBin)
	require.True(t, slices.IsSortedFunc(version2.Aggs[0].SamplesByBin, func(a, b seq.AggBinSamples) int {
		return seq.CompareAggBin(a.Bin, b.Bin)
	}))
}

func getRandomQPR(size int) seq.QPR {
	curTime := time.Now()
	getTime := func() time.Time {
		curTime = curTime.Add(500 * time.Microsecond)
		return curTime
	}

	var ids seq.IDSources
	for i := 0; i < size; i++ {
		mid := getTime()
		rid := rand.N[uint64](math.MaxUint64)
		src := rand.N[uint64](math.MaxUint64)
		ids = append(ids, seq.IDSource{ID: seq.NewID(mid, rid), Source: src})
	}

	var aggs []seq.AggregatableSamples
	for i := 0; i < size; i++ {
		hist := aggSamplesFromMap(map[string]uint64{"_not_exists": 1})
		aggs = append(aggs, hist)
	}

	hists := make(map[seq.MID]uint64)
	for i := 0; i < size; i++ {
		hists[seq.NewID(getTime(), uint64(i%10)).MID]++
	}

	var errs []seq.ErrorSource
	for i := 0; i < rand.N(100); i++ {
		src := rand.N[uint64](math.MaxUint64)
		errs = append(errs, seq.ErrorSource{ErrStr: "error", Source: src})
	}

	return seq.QPR{
		IDs:       ids,
		Histogram: hists,
		Aggs:      aggs,
		Total:     uint64(size),
		Errors:    errs,
	}
}

func aggSamplesFromMap(other map[string]uint64) seq.AggregatableSamples {
	samplesByBin := make([]seq.AggBinSamples, 0, len(other))
	for k, cnt := range other {
		hist := seq.NewSamplesContainers()
		hist.Total = int64(cnt)
		samplesByBin = append(samplesByBin, seq.AggBinSamples{
			Bin:     seq.AggBin{Token: k},
			Samples: *hist,
		})
	}
	seq.SortAggBinSamples(samplesByBin)
	return seq.AggregatableSamples{
		SamplesByBin: samplesByBin,
		NotExists:    int64(other["_not_exists"]),
	}
}
