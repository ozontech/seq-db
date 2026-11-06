package asyncsearcher

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
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
		qpr.ToCompressed()
		require.Equal(t, qpr.IDs, out.IDs)
		require.Equal(t, qpr.Histogram, out.Histogram)
		require.Equal(t, qpr.Total, out.Total)
		require.Equal(t, qpr.Errors, out.Errors)
		require.Len(t, out.Aggs, len(qpr.Aggs))
		for i := range qpr.Aggs {
			requireAggsEqual(t, qpr.Aggs[i], out.Aggs[i])
		}
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
		Aggs:      []seq.AggregatableSamples{testAggSamples()},
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

func TestUnmarshalAggsDoesNotAliasPooledBuffer(t *testing.T) {
	agg := testAgg(seq.AggFuncCount, 1,
		testAggRow{tokens: []string{"a"}, total: 1},
		testAggRow{tokens: []string{"b"}, total: 2},
	)
	encoded := marshalAggs(nil, []seq.AggregatableSamples{agg})

	decoded, tail, err := unmarshalAggs(nil, encoded, qprBinVersion3, []seq.AggFunc{seq.AggFuncCount})
	require.NoError(t, err)
	require.Len(t, tail, 0)
	require.Len(t, decoded, 1)

	pooled := aggsCompressBufferPool.Get()
	pooled.B = pooled.B[:cap(pooled.B)]
	for i := range pooled.B {
		pooled.B[i] = 0xff
	}
	aggsCompressBufferPool.Put(pooled)

	var got []string
	it := decoded[0].Tokens[0].NewIter()
	for it.Next() {
		got = append(got, string(it.Row()))
	}
	require.Equal(t, []string{"a", "b"}, got)

	totals := decoded[0].Values.Totals.NewIter()
	var values []int64
	for totals.Next() {
		values = append(values, totals.Value())
	}
	require.Equal(t, []int64{1, 2}, values)
}

type testAggRow struct {
	tokens    []string
	mid       seq.MID
	total     int64
	notExists int64
	sum       float64
	min       float64
	max       float64
	samples   []float64
	uniqueIDs []uint32
}

func testAgg(fn seq.AggFunc, width int, rows ...testAggRow) seq.AggregatableSamples {
	agg := seq.NewAggregatableSamples(fn, width, len(rows))
	withMIDs := false
	for _, row := range rows {
		withMIDs = withMIDs || row.mid != consts.DummyMID
	}
	if withMIDs {
		agg.MIDs = seq.NewInt64Column(len(rows))
	}
	for _, row := range rows {
		for i, token := range row.tokens {
			agg.Tokens[i].AppendString(token)
		}
		if withMIDs {
			agg.MIDs.Append(int64(row.mid))
		}
		switch fn {
		case seq.AggFuncCount:
			agg.Values.Totals.Append(row.total)
		case seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg:
			appendNumericAggRow(&agg.Values, row)
		case seq.AggFuncQuantile:
			appendNumericAggRow(&agg.Values, row)
			agg.Values.Quantiles.Append(row.samples)
		case seq.AggFuncUniqueCount:
			agg.Values.Totals.Append(row.total)
			agg.Values.NotExists.Append(row.notExists)
			agg.Values.UniqueIDs.Append(row.uniqueIDs)
		case seq.AggFuncUnique:
		}
	}
	return *agg
}

func appendNumericAggRow(values *seq.AggValues, row testAggRow) {
	values.Totals.Append(row.total)
	values.NotExists.Append(row.notExists)
	values.Sums.Append(row.sum)
	values.Mins.Append(row.min)
	values.Maxs.Append(row.max)
}

func testAggSamples() seq.AggregatableSamples {
	agg := testAgg(seq.AggFuncQuantile, 1,
		testAggRow{tokens: []string{"seq-db store"}, mid: 2, total: 1, notExists: 7, sum: 794, min: 3, max: 5, samples: []float64{324}},
		testAggRow{tokens: []string{"seq-db store"}, mid: 1, total: 2, notExists: 3, sum: 544, min: 2, max: 6, samples: []float64{324}},
		testAggRow{tokens: []string{"_not_exists"}, total: 1},
		testAggRow{tokens: []string{"seq-db proxy"}, total: 1, sum: 100, min: 0, max: 100, samples: []float64{100}},
	)
	agg.NotExists = 5412
	return agg
}

func marshalLegacyQPR(q *seq.QPR, dst []byte, version qprBinVersion) []byte {
	dst = append(dst, uint8(version))

	blocksLenPos := len(dst)
	dst = append(dst, make([]byte, 8)...)
	n := len(dst)
	dst = marshalIDsBlocks(dst, q.IDs)
	blocksLen := len(dst) - n
	binary.BigEndian.PutUint64(dst[blocksLenPos:], uint64(blocksLen))

	dst = marshalHistogram(dst, q.Histogram)
	dst = marshalLegacyAggs(dst, q.Aggs, version)
	dst = be.AppendUint64(dst, q.Total)
	dst = marshalErrorSource(dst, q.Errors)
	return dst
}

func marshalLegacyAggs(dst []byte, aggs []seq.AggregatableSamples, version qprBinVersion) []byte {
	headerPos := len(dst)
	header := aggsBlockHeader{}
	dst = (&header).Marshal(dst)

	for i := range aggs {
		dst = marshalLegacyAggregatableSamples(aggs[i], dst, version)
	}

	header = aggsBlockHeader{
		Codec:  aggsCodecNone,
		Length: uint64(len(dst) - headerPos - len((&aggsBlockHeader{}).Marshal(nil))),
	}
	_ = (&header).Marshal(dst[:headerPos])
	return dst
}

func marshalLegacyAggregatableSamples(s seq.AggregatableSamples, dst []byte, version qprBinVersion) []byte {
	rows := s.Count()
	dst = be.AppendUint64(dst, uint64(rows))
	prevMID := seq.MID(0)
	var tokens seq.StringColumnIter
	if len(s.Tokens) > 0 {
		tokens = s.Tokens[0].NewIter()
	}
	var mids seq.Int64ColumnIter
	if s.MIDs != nil {
		mids = s.MIDs.NewIter()
	}
	for i := range rows {
		var token string
		if tokens.Next() {
			token = string(tokens.Row())
		}
		mid := seq.MID(consts.DummyMID)
		if mids != nil && mids.Next() {
			mid = seq.MID(mids.Value())
		}
		sample := seq.AggBin{MID: mid, Token: token}
		delta := int64(sample.MID) - int64(prevMID)
		prevMID = sample.MID
		dst = binary.AppendVarint(dst, delta)
		dst = binary.AppendUvarint(dst, uint64(len(sample.Token)))
		dst = append(dst, sample.Token...)
		dst = marshalLegacySamplesContainer(legacyRowSamples(&s.Values, i), dst)
	}
	dst = be.AppendUint64(dst, uint64(s.NotExists))
	return dst
}

func legacyRowSamples(values *seq.AggValues, row int) *seq.SamplesContainer {
	samples := &seq.SamplesContainer{}
	switch values.Func {
	case seq.AggFuncCount, seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg, seq.AggFuncQuantile, seq.AggFuncUniqueCount:
		samples.Total = values.Totals.At(row)
		if values.Func != seq.AggFuncCount {
			samples.NotExists = values.NotExists.At(row)
		}
	}
	switch values.Func {
	case seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg, seq.AggFuncQuantile:
		samples.Sum = values.Sums.At(row)
		samples.Min = values.Mins.At(row)
		samples.Max = values.Maxs.At(row)
		if values.Func == seq.AggFuncQuantile {
			samples.Samples = values.Quantiles.At(row)
		}
	case seq.AggFuncUniqueCount:
		samples.Values = make(map[uint32]struct{}, len(values.UniqueIDs.At(row)))
		for _, id := range values.UniqueIDs.At(row) {
			samples.Values[id] = struct{}{}
		}
	}
	return samples
}

func marshalLegacySamplesContainer(h *seq.SamplesContainer, dst []byte) []byte {
	dst = be.AppendUint64(dst, math.Float64bits(h.Min))
	dst = be.AppendUint64(dst, math.Float64bits(h.Max))
	dst = be.AppendUint64(dst, math.Float64bits(h.Sum))
	dst = binary.AppendUvarint(dst, uint64(h.Total))
	dst = binary.AppendUvarint(dst, uint64(h.NotExists))

	dst = binary.AppendUvarint(dst, uint64(len(h.Samples)))
	for _, v := range h.Samples {
		dst = be.AppendUint64(dst, math.Float64bits(v))
	}
	return dst
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
		Aggs: []seq.AggregatableSamples{{
			Tokens: []seq.StringColumn{stringColumn("_not_exists", "seq-db store", "seq-db store")},
			MIDs: midsColumn(
				consts.DummyMID,
				seq.MID(1761812502456),
				seq.MID(1761812502953),
			),
			Values: seq.NewAggValuesFromSamples(seq.AggFuncQuantile, []seq.SamplesContainer{
				{Total: 1},
				{Min: 2, Max: 6, Sum: 544, Total: 2, NotExists: 3, Samples: []float64{324}},
				{Min: 3, Max: 5, Sum: 794, Total: 1, NotExists: 7, Samples: []float64{324}},
			}),
			NotExists: 5412,
		}},
	}

	rawQPR := marshalLegacyQPR(&qpr, nil, qprBinVersion1)

	var outQPR seq.QPR
	tail, err := unmarshalQPR(&outQPR, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Equal(t, seq.MID(1761812502573000000), outQPR.IDs[0].ID.MID)

	require.Len(t, outQPR.Histogram, 2)
	require.Equal(t, uint64(433), outQPR.Histogram[seq.MID(1761812502573000000)])
	require.Equal(t, uint64(743), outQPR.Histogram[seq.MID(1761812502463000000)])

	require.Len(t, outQPR.Aggs, 1)
	agg := outQPR.Aggs[0]
	require.Equal(t, 3, agg.MIDs.Len())
	require.Equal(t, []string{"seq-db store", "seq-db store", "_not_exists"}, stringColumnValues(agg.Tokens[0]))
	require.Equal(t, []int64{
		int64(seq.MID(1761812502953000000)),
		int64(seq.MID(1761812502456000000)),
		int64(consts.DummyMID),
	}, int64ColumnValues(agg.MIDs))
	require.Equal(t, []int64{1, 2, 1}, int64ColumnValues(agg.Values.Totals))
}

func TestQPRSortsAggBins(t *testing.T) {
	sortedAgg := testAgg(seq.AggFuncCount, 1,
		testAggRow{tokens: []string{"a"}, mid: 3},
		testAggRow{tokens: []string{"b"}, mid: 2},
		testAggRow{tokens: []string{"a"}, mid: 1},
	)
	sortedQPR := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{sortedAgg},
	}

	rawQPR := marshalQPR(&sortedQPR, nil)
	var version3 seq.QPR
	tail, err := unmarshalQPR(&version3, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	requireAggsEqual(t, sortedQPR.Aggs[0], version3.Aggs[0])

	unsortedQPR := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs: []seq.AggregatableSamples{{
			Tokens: []seq.StringColumn{stringColumn("b", "a", "a")},
			MIDs:   midsColumn(2, 3, 1),
			Values: seq.NewAggValuesFromSamples(seq.AggFuncCount, []seq.SamplesContainer{
				{}, {}, {},
			}),
		}},
	}
	rawQPR = slices.Clone(marshalLegacyQPR(&unsortedQPR, nil, qprBinVersion2))

	var version2 seq.QPR
	tail, err = unmarshalQPR(&version2, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	requireAggsEqual(t, sortedAgg, version2.Aggs[0])
}

func TestQPRVersion3ColumnarAggregations(t *testing.T) {
	for _, fn := range []seq.AggFunc{
		seq.AggFuncCount,
		seq.AggFuncSum,
		seq.AggFuncMin,
		seq.AggFuncMax,
		seq.AggFuncAvg,
		seq.AggFuncQuantile,
		seq.AggFuncUnique,
		seq.AggFuncUniqueCount,
	} {
		t.Run(fn.String(), func(t *testing.T) {
			first := testAggRow{
				tokens:    []string{"a"},
				mid:       2,
				total:     2,
				notExists: 5,
				sum:       4,
				min:       1,
				max:       3,
				samples:   []float64{1, 3},
				uniqueIDs: []uint32{1, 2},
			}
			second := testAggRow{
				tokens:    []string{"b"},
				mid:       1,
				total:     1,
				notExists: 6,
				sum:       6,
				min:       2,
				max:       4,
				samples:   []float64{2, 4},
				uniqueIDs: []uint32{2, 3},
			}
			expected := testAgg(fn, 1, first, second)

			qpr := seq.QPR{
				Histogram: map[seq.MID]uint64{},
				Aggs:      []seq.AggregatableSamples{expected},
			}
			rawQPR := marshalQPR(&qpr, nil)
			require.Equal(t, qprBinVersion3, qprBinVersion(rawQPR[0]))

			var actual seq.QPR
			tail, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
			require.NoError(t, err)
			require.Equal(t, 0, len(tail))
			require.Len(t, actual.Aggs, 1)
			requireAggsEqual(t, expected, actual.Aggs[0])
		})
	}
}

func TestQPRVersion3Compatibility(t *testing.T) {
	expected := testAggSamples()
	qpr := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{expected},
	}

	rawQPR := marshalQPR(&qpr, nil)
	var actual seq.QPR
	tail, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Len(t, actual.Aggs, 1)
	requireAggsEqual(t, expected, actual.Aggs[0])
}

func TestQPRVersion3MultipleTokenColumns(t *testing.T) {
	expected := testAgg(seq.AggFuncCount, 2,
		testAggRow{tokens: []string{"a", "z"}, mid: 2, total: 1},
		testAggRow{tokens: []string{"b", "a"}, mid: 1, total: 2},
	)
	qpr := seq.QPR{
		Histogram: map[seq.MID]uint64{},
		Aggs:      []seq.AggregatableSamples{expected},
	}

	rawQPR := marshalQPR(&qpr, nil)
	var actual seq.QPR
	tail, err := unmarshalQPR(&actual, rawQPR, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, 0, len(tail))
	require.Len(t, actual.Aggs, 1)
	requireAggsEqual(t, expected, actual.Aggs[0])
}

func requireAggsEqual(t *testing.T, expected, actual seq.AggregatableSamples) {
	t.Helper()
	require.Equal(t, expected.Count(), actual.Count())
	require.Equal(t, expected.Values.Func, actual.Values.Func)
	require.Equal(t, expected.NotExists, actual.NotExists)
	require.Equal(t, expected.ValuesPool, actual.ValuesPool)
	require.Equal(t, testAggRows(expected), testAggRows(actual))
}

func testAggRows(agg seq.AggregatableSamples) []testAggRow {
	rows := make([]testAggRow, 0, agg.Count())
	iters := make([]seq.StringColumnIter, len(agg.Tokens))
	var mids seq.Int64ColumnIter
	var totals seq.Int64ColumnIter
	var notExists seq.Int64ColumnIter
	for i := range iters {
		iters[i] = agg.Tokens[i].NewIter()
	}
	for i := range iters {
		if !iters[i].Next() {
			return rows
		}
	}
	if agg.MIDs != nil {
		mids = agg.MIDs.NewIter()
	}
	if agg.Values.Totals != nil {
		totals = agg.Values.Totals.NewIter()
	}
	if agg.Values.NotExists != nil {
		notExists = agg.Values.NotExists.NewIter()
	}
	for row := range agg.Count() {
		var mid seq.MID
		if mids != nil && mids.Next() {
			mid = seq.MID(mids.Value())
		}
		item := testAggRow{mid: mid}
		for i := range iters {
			item.tokens = append(item.tokens, string(iters[i].Row()))
		}
		switch agg.Values.Func {
		case seq.AggFuncCount:
			if totals.Next() {
				item.total = totals.Value()
			}
		case seq.AggFuncSum, seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncAvg:
			if totals.Next() {
				item.total = totals.Value()
			}
			if notExists.Next() {
				item.notExists = notExists.Value()
			}
			switch agg.Values.Func {
			case seq.AggFuncSum, seq.AggFuncAvg:
				item.sum = agg.Values.Sums.At(row)
			case seq.AggFuncMin:
				item.min = agg.Values.Mins.At(row)
			case seq.AggFuncMax:
				item.max = agg.Values.Maxs.At(row)
			}
		case seq.AggFuncQuantile:
			if totals.Next() {
				item.total = totals.Value()
			}
			if notExists.Next() {
				item.notExists = notExists.Value()
			}
			item.min = agg.Values.Mins.At(row)
			item.max = agg.Values.Maxs.At(row)
			item.samples = agg.Values.Quantiles.At(row)
		case seq.AggFuncUniqueCount:
			if totals.Next() {
				item.total = totals.Value()
			}
			if notExists.Next() {
				item.notExists = notExists.Value()
			}
			item.uniqueIDs = agg.Values.UniqueIDs.At(row)
		}
		rows = append(rows, item)
		for i := range iters {
			iters[i].Next()
		}
	}
	return rows
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

func stringColumn(values ...string) seq.StringColumn {
	column := seq.NewStringColumn(len(values))
	for _, value := range values {
		column.AppendString(value)
	}
	return column
}

func midsColumn(values ...seq.MID) seq.Int64Column {
	column := seq.NewInt64Column(len(values))
	for _, value := range values {
		column.Append(int64(value))
	}
	return column
}

func stringColumnValues(column seq.StringColumn) []string {
	values := make([]string, 0, column.Len())
	it := column.NewIter()
	for it.Next() {
		values = append(values, string(it.Row()))
	}
	return values
}

func int64ColumnValues(column seq.Int64Column) []int64 {
	values := make([]int64, 0, column.Len())
	it := column.NewIter()
	for it.Next() {
		values = append(values, it.Value())
	}
	return values
}

func aggSamplesFromMap(other map[string]uint64) seq.AggregatableSamples {
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
	result := testAgg(seq.AggFuncCount, 1, rows...)
	result.NotExists = int64(other["_not_exists"])
	return result
}
