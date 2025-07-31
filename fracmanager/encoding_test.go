package fracmanager

import (
	"math"
	"math/rand/v2"
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
				SamplesByBin: map[seq.AggBin]*seq.SamplesContainer{
					{Token: "_not_exists"}: {
						Total: 1,
					},
					{Token: "seq-db proxy"}: {
						Min:       0,
						Max:       100,
						Sum:       100,
						Total:     1,
						NotExists: 0,
						Samples:   []float64{100},
					},
					{Token: "seq-db store", MID: seq.MID(1)}: {
						Min:       3,
						Max:       5,
						Sum:       794,
						Total:     1,
						NotExists: 7,
						Samples:   []float64{324},
					},
					{Token: "seq-db store", MID: seq.MID(2)}: {
						Min:       2,
						Max:       6,
						Sum:       544,
						Total:     2,
						NotExists: 3,
						Samples:   []float64{324},
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
	samplesByBin := make(map[seq.AggBin]*seq.SamplesContainer, len(other))
	for k, cnt := range other {
		hist := seq.NewSamplesContainers()
		hist.Total = int64(cnt)
		samplesByBin[seq.AggBin{Token: k}] = hist
	}
	return seq.AggregatableSamples{
		SamplesByBin: samplesByBin,
		NotExists:    int64(other["_not_exists"]),
	}
}
