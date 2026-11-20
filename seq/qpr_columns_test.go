package seq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/util"
)

func TestArraysColumnAppendTo(t *testing.T) {
	quantiles := ArraysColumn[float64]{
		Values:  []float64{1, -2},
		Offsets: []int{0, 1, 2},
	}
	quantileData := quantiles.AppendTo(nil)
	require.Len(t, quantileData, arraysColumnHeaderSize+2*sizeOfFloat64+3*util.SizeOfInt64)
	require.Equal(t, arraysColumnMagic, quantileData[0])
	require.Equal(t, arraysColumnFormatVersion, quantileData[1])
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(quantileData[4:8]))
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(quantileData[8:12]))
	decodedQuantiles, err := UnmarshalArraysColumn[float64](quantileData)
	require.NoError(t, err)
	require.Equal(t, quantiles, decodedQuantiles)

	uniqueIDs := ArraysColumn[uint32]{
		Values:  []uint32{1, 70000},
		Offsets: []int{0, 2},
	}
	uniqueIDsData := uniqueIDs.AppendTo(nil)
	require.Len(t, uniqueIDsData, arraysColumnHeaderSize+2*sizeOfUint32+2*util.SizeOfInt64)
	require.Equal(t, arraysColumnMagic, uniqueIDsData[0])
	require.Equal(t, arraysColumnFormatVersion, uniqueIDsData[1])
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(uniqueIDsData[4:8]))
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(uniqueIDsData[8:12]))
	decodedUniqueIDs, err := UnmarshalArraysColumn[uint32](uniqueIDsData)
	require.NoError(t, err)
	require.Equal(t, uniqueIDs, decodedUniqueIDs)

	emptyQuantiles, err := UnmarshalArraysColumn[float64](ArraysColumn[float64]{}.AppendTo(nil))
	require.NoError(t, err)
	require.Nil(t, emptyQuantiles.Values)
	require.Nil(t, emptyQuantiles.Offsets)
}

func TestArraysColumnEmptyOffsets(t *testing.T) {
	column := ArraysColumn[int]{Offsets: []int{}}
	require.Zero(t, column.Len())
	require.Empty(t, column.At(0))
	require.NoError(t, column.Validate(0))

	column.Append([]int{1})
	column.Append([]int{2})
	require.Equal(t, 2, column.Len())
	require.Equal(t, []int{1}, column.At(0))
	require.Equal(t, []int{2}, column.At(1))
	require.NoError(t, column.Validate(2))
}

func TestUnmarshalArraysColumnErrors(t *testing.T) {
	valid := ArraysColumn[float64]{
		Values:  []float64{1},
		Offsets: []int{0, 1},
	}.AppendTo(nil)

	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{"empty", nil, "not enough data"},
		{"short header", valid[:arraysColumnHeaderSize-1], "not enough data"},
		{"bad magic", append([]byte{0}, valid[1:]...), "invalid array column magic"},
		{"bad version", append([]byte{arraysColumnMagic, 0}, valid[2:]...), "unsupported array column format version"},
		{"truncated data", valid[:len(valid)-1], "malformed array column data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalArraysColumn[float64](tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}

	invalidOffset := ArraysColumn[float64]{Values: []float64{1}, Offsets: []int{0}}.AppendTo(nil)
	require.Len(t, invalidOffset, arraysColumnHeaderSize+sizeOfFloat64+util.SizeOfInt64)
	binary.LittleEndian.PutUint64(invalidOffset[len(invalidOffset)-util.SizeOfInt64:], math.MaxUint64)
	_, err := UnmarshalArraysColumn[float64](invalidOffset)
	require.ErrorContains(t, err, "invalid array offset")

	_, err = UnmarshalArraysColumn[int](valid)
	require.ErrorContains(t, err, "unsupported array column value type")
}

func requireColumnRows(t *testing.T, col StringColumn, want ...string) {
	t.Helper()
	var got []string
	it := col.NewIter()
	rows := 0
	for it.Next() {
		got = append(got, string(it.Row()))
		rows++
	}
	require.False(t, it.Next(), "iteration must stay exhausted")
	require.Equal(t, col.Len(), rows, "iterator row count matches Len()")
	require.Equal(t, want, got)
}

func buildStringColumn(t *testing.T, values ...string) StringColumn {
	t.Helper()
	col := NewStringColumn(len(values))
	for _, v := range values {
		col.AppendString(v)
	}
	requireColumnRows(t, col, values...)
	return col
}

func checkStringColumnRoundTrip(t *testing.T, values []string) {
	t.Helper()
	col := buildStringColumn(t, values...)
	decoded, err := UnmarshalStringColumn(col.AppendTo(nil))
	require.NoError(t, err)
	requireColumnRows(t, decoded, values...)
}

func TestStringColumnIteration(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	randomValues := func(rows int) []string {
		values := make([]string, rows)
		for i := range values {
			n := rng.Intn(64)
			if rng.Intn(10) == 0 {
				n = rng.Intn(40 * 1024) // occasional large row
			}
			buf := make([]byte, n)
			rng.Read(buf)
			values[i] = string(buf)
		}
		return values
	}
	tests := []struct {
		name   string
		values []string
	}{
		{"empty", nil},
		{"single row", []string{"a"}},
		{"empty rows", []string{"", "", ""}},
		{"mixed empty", []string{"a", "", "abc", ""}},
		{"duplicates", []string{"b", "a", "a", "b", "a"}},
		{"unicode", []string{"héllo wörld", "привет", "日本語", ""}},
	}
	for _, rows := range []int{1, 2, 127, 128, 129, 1000, 5000} {
		tests = append(tests, struct {
			name   string
			values []string
		}{fmt.Sprintf("random rows=%d", rows), randomValues(rows)})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkStringColumnRoundTrip(t, tt.values)
		})
	}
}

func TestStringColumnIteratorsAreIndependent(t *testing.T) {
	values := []string{"a", "bb", "ccc", "dddd"}
	col := buildStringColumn(t, values...)
	a := col.NewIter()
	b := col.NewIter()
	require.True(t, a.Next())
	require.True(t, b.Next())
	i := 0
	for {
		require.Equal(t, values[i], string(a.Row()))
		require.Equal(t, values[i], string(b.Row()))
		aNext, bNext := a.Next(), b.Next()
		require.Equal(t, aNext, bNext)
		if !aNext {
			break
		}
		i++
	}
	require.Equal(t, len(values)-1, i)
}

func TestStringColumnOversizedRow(t *testing.T) {
	big := bytes.Repeat([]byte("oversized!"), 10*1024) // 100KB
	col := buildStringColumn(t, "small", string(big), "tail")
	decoded, err := UnmarshalStringColumn(col.AppendTo(nil))
	require.NoError(t, err)
	requireColumnRows(t, decoded, "small", string(big), "tail")
}

func TestStringColumnEmptyEncoding(t *testing.T) {
	col := NewStringColumn(0)
	encoded := col.AppendTo(nil)
	require.Len(t, encoded, stringColumnHeaderSize)

	decoded, err := UnmarshalStringColumn(encoded)
	require.NoError(t, err)
	require.Zero(t, decoded.Len())
	requireColumnRows(t, decoded)
}

func TestStringColumnEncoding(t *testing.T) {
	col := buildStringColumn(t, "aaa", "bbbb")
	encoded := col.AppendTo(nil)
	require.Len(t, encoded, stringColumnHeaderSize+3*sizeOfUint32+7)
	require.Equal(t, stringColumnMagic, encoded[0])
	require.Equal(t, stringColumnFormatVersion, encoded[1])
	require.Equal(t, uint64(2), binary.LittleEndian.Uint64(encoded[4:12]))
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(encoded[stringColumnHeaderSize:]))
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(encoded[stringColumnHeaderSize+sizeOfUint32:]))
	require.Equal(t, uint32(7), binary.LittleEndian.Uint32(encoded[stringColumnHeaderSize+2*sizeOfUint32:]))
	require.Equal(t, "aaabbbb", string(encoded[stringColumnHeaderSize+3*sizeOfUint32:]))

	decoded, err := UnmarshalStringColumn(encoded)
	require.NoError(t, err)
	require.Equal(t, encoded, decoded.AppendTo(nil))
}

func TestUnmarshalStringColumnAliasesInput(t *testing.T) {
	encoded := buildStringColumn(t, "aaa", "bbbb").AppendTo(nil)
	decoded, err := UnmarshalStringColumn(encoded)
	require.NoError(t, err)
	dataStart := stringColumnHeaderSize + 3*sizeOfUint32
	require.Same(t, &encoded[dataStart], &decoded.(*RawStringColumn).data[0])
}

func TestUnmarshalStringColumnErrors(t *testing.T) {
	valid := buildStringColumn(t, "a", "b", "c").AppendTo(nil)

	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{"empty", nil, "not enough data"},
		{"short header", valid[:10], "not enough data"},
		{"bad magic", append([]byte{0x00}, valid[1:]...), "invalid string column magic"},
		{"bad version", append([]byte{valid[0], 0x00}, valid[2:]...), "unsupported string column format version"},
		{"bad codec", append([]byte{valid[0], valid[1], 0x01}, valid[3:]...), "unsupported string column codec"},
		{"truncated offsets", valid[:stringColumnHeaderSize+sizeOfUint32], "truncated string column offsets"},
		{"truncated data", valid[:len(valid)-1], "data has"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalStringColumn(tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}

	corrupt := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint64(corrupt[4:12], math.MaxUint64)
	_, err := UnmarshalStringColumn(corrupt)
	require.ErrorContains(t, err, "too many rows")

	offsetsStart := stringColumnHeaderSize
	corrupt = append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(corrupt[offsetsStart+sizeOfUint32:], 100) // second offset beyond data end
	_, err = UnmarshalStringColumn(corrupt)
	require.ErrorContains(t, err, "not monotonic")

	corrupt = append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(corrupt[stringColumnHeaderSize+3*sizeOfUint32:], 100) // final offset beyond data end
	_, err = UnmarshalStringColumn(corrupt)
	require.ErrorContains(t, err, "data has")
}

func requireInt64Values(t *testing.T, col Int64Column, want ...int64) {
	t.Helper()
	got := make([]int64, 0, len(want))
	it := col.NewIter()
	rows := 0
	for it.Next() {
		got = append(got, it.Value())
		rows++
	}
	require.False(t, it.Next(), "iteration must stay exhausted")
	require.Equal(t, col.Len(), rows, "iterator count matches Len()")
	if len(want) == 0 {
		require.Empty(t, got)
		return
	}
	require.Equal(t, want, got)
}

func checkInt64ColumnRoundTrip(t *testing.T, values []int64) {
	t.Helper()
	col := NewInt64Column(len(values))
	for _, v := range values {
		col.Append(v)
	}
	requireInt64Values(t, col, values...)
	decoded, err := UnmarshalInt64Column(col.AppendTo(nil))
	require.NoError(t, err)
	requireInt64Values(t, decoded, values...)
}

func TestInt64ColumnIteration(t *testing.T) {
	for _, rows := range []int{0, 1, 2, 127, 128, 129, 1000, 5000} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			checkInt64ColumnRoundTrip(t, genInt64Values(rows))
		})
	}
}

func TestInt64ColumnRawAccess(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{"single", []int64{42}},
		{"zeros", make([]int64, 1000)},
		{"counts", []int64{5, 3, 1200, 1, 42, 0, 999, 3}},
		{"negative values", []int64{-5, 3, -1200, 1, 42, 0, -999, 3}},
		{"many values", genInt64Values(5000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := NewInt64Column(len(tt.values))
			for _, v := range tt.values {
				col.Append(v)
			}
			for i, v := range tt.values {
				require.Equal(t, v, col.At(i))
			}

			requireInt64Values(t, col, tt.values...)
		})
	}
}

func TestNewEmptyInt64Column(t *testing.T) {
	column := NewEmptyInt64Column()
	require.IsType(t, &RawInt64Column{}, column)
	require.Zero(t, column.Len())
	require.Equal(t, int(unsafe.Sizeof(RawInt64Column{})), column.MemUsage())
	require.False(t, column.NewIter().Next())

	encoded := column.AppendTo(nil)
	require.Len(t, encoded, int64ColumnHeaderSize)
	decoded, err := UnmarshalInt64Column(encoded)
	require.NoError(t, err)
	requireInt64Values(t, decoded)

	allocations := testing.AllocsPerRun(100, func() {
		column = NewEmptyInt64Column()
	})
	require.LessOrEqualf(t, allocations, float64(1), "empty column allocations: %v", allocations)

	allocations = testing.AllocsPerRun(100, func() {
		encoded = column.AppendTo(encoded[:0])
	})
	require.Zero(t, allocations)
}

func TestColumnMemUsageTracksRetainedCapacity(t *testing.T) {
	stringColumn := NewStringColumn(0)
	rawString := stringColumn.(*RawStringColumn)
	rawString.data = make([]byte, 0, 64)
	rawString.offsets = make([]uint32, 0, 10)
	require.Equal(t,
		int(unsafe.Sizeof(*rawString))+
			cap(rawString.data)+
			sizeOfUint32*cap(rawString.offsets),
		stringColumn.MemUsage(),
	)

	intColumn := NewInt64Column(2)
	rawInt := intColumn.(*RawInt64Column)
	require.Equal(t,
		int(unsafe.Sizeof(*rawInt))+cap(rawInt.values)*sizeOfInt64,
		intColumn.MemUsage(),
	)
}

func TestEmptyAggregatableSamplesPerFunc(t *testing.T) {
	for _, fn := range []AggFunc{
		AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax,
		AggFuncAvg, AggFuncQuantile, AggFuncUnique, AggFuncUniqueCount,
	} {
		agg := NewAggregatableSamples(fn, 1, 0)
		require.Len(t, agg.Tokens, 1)
		require.NotNil(t, agg.Values.Totals)
		require.NotNil(t, agg.Values.NotExists)

		result := agg.Aggregate(AggregateArgs{Func: fn, Quantiles: []float64{0.5}})
		require.Emptyf(t, result.Buckets, "func %d", fn)
		agg.MemUsage()
	}
}

func TestInt64ColumnAbsent(t *testing.T) {
	var col Int64Column
	require.Nil(t, col)
	agg := &AggregatableSamples{MIDs: col, Values: newAggValues(AggFuncUnique, 0)}
	require.Zero(t, agg.Count())
	qpr := &QPR{Aggs: []AggregatableSamples{*agg}}
	require.NotPanics(t, func() {
		qpr.Aggs[0].Values.Totals.MemUsage()
	})
	require.Equal(t, int(unsafe.Sizeof(RawInt64Column{})), agg.Values.Totals.MemUsage())
	require.Equal(t, int(unsafe.Sizeof(RawInt64Column{})), agg.Values.NotExists.MemUsage())
}

func TestUnmarshalInt64ColumnErrors(t *testing.T) {
	col := NewInt64Column(3)
	col.Append(1)
	col.Append(2)
	col.Append(3)
	valid := col.AppendTo(nil)

	_, err := UnmarshalInt64Column(nil)
	require.ErrorContains(t, err, "not enough data")
	_, err = UnmarshalInt64Column([]byte{0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.ErrorContains(t, err, "magic")
	_, err = UnmarshalInt64Column([]byte{0x49, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.ErrorContains(t, err, "unsupported int64 column format version")
	_, err = UnmarshalInt64Column([]byte{0x49, 0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.ErrorContains(t, err, "unsupported int64 column codec")

	corrupt := append([]byte(nil), valid...)
	corrupt[4] = 42 // patch the values count
	_, err = UnmarshalInt64Column(corrupt)
	require.ErrorContains(t, err, "malformed int64 column data")

	_, err = UnmarshalInt64Column(valid[:len(valid)-1])
	require.ErrorContains(t, err, "malformed int64 column data")
}

func genInt64Values(n int) []int64 {
	rng := rand.New(rand.NewSource(9))
	values := make([]int64, n)
	for i := range values {
		switch rng.Intn(4) {
		case 0:
			values[i] = int64(rng.Int63n(1 << 20))
		case 1:
			values[i] = -int64(rng.Int63n(1 << 20))
		case 2:
			values[i] = 0
		default:
			values[i] = int64(rng.Int63n(100))
		}
	}
	return values
}

func requireFloat64Values(t *testing.T, column Float64Column, want ...float64) {
	t.Helper()
	require.Equal(t, len(want), column.Len(), "column count matches expected values")

	it := column.NewIter()
	got := make([]float64, 0, len(want))
	for it.Next() {
		got = append(got, it.Value())
	}
	require.Len(t, got, len(want))
	for i := range want {
		if math.IsNaN(want[i]) {
			require.True(t, math.IsNaN(got[i]), "value %d must be NaN", i)
			continue
		}
		require.Equal(t, want[i], got[i], "value %d", i)
	}
}

func TestFloat64ColumnAccess(t *testing.T) {
	want := []float64{1, -2, math.NaN(), math.Inf(-1), math.Inf(1)}
	column := NewFloat64Column(len(want))

	for _, value := range want {
		column.Append(value)
	}
	for i, value := range want {
		if math.IsNaN(value) {
			require.True(t, math.IsNaN(column.At(i)))
			continue
		}
		require.Equal(t, value, column.At(i))
	}

	column.Add(0, 4)
	column.Add(1, 2)
	want[0] += 4
	want[1] += 2
	requireFloat64Values(t, column, want...)
}

func TestFloat64ColumnEncoding(t *testing.T) {
	want := []float64{1, -2, math.NaN(), math.Inf(-1), math.Inf(1)}
	column := NewFloat64Column(len(want))
	for _, value := range want {
		column.Append(value)
	}

	encoded := column.AppendTo(nil)
	require.Len(t, encoded, float64ColumnHeaderSize+len(want)*sizeOfFloat64)
	require.Equal(t, float64ColumnMagic, encoded[0])
	require.Equal(t, float64ColumnFormatVersion, encoded[1])
	require.Equal(t, uint32(len(want)), binary.LittleEndian.Uint32(encoded[4:8]))

	decoded, err := UnmarshalFloat64Column(encoded)
	require.NoError(t, err)
	requireFloat64Values(t, decoded, want...)
	require.Equal(t, encoded, decoded.AppendTo(nil))
}

func TestFloat64ColumnEmptyEncoding(t *testing.T) {
	column := NewFloat64Column(0)
	encoded := column.AppendTo(nil)
	require.Len(t, encoded, float64ColumnHeaderSize)
	require.Equal(t, float64ColumnMagic, encoded[0])
	require.Equal(t, float64ColumnFormatVersion, encoded[1])
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(encoded[4:8]))

	decoded, err := UnmarshalFloat64Column(encoded)
	require.NoError(t, err)
	require.Zero(t, decoded.Len())
	require.Equal(t, encoded, decoded.AppendTo(nil))
}

func TestUnmarshalFloat64ColumnErrors(t *testing.T) {
	column := NewFloat64Column(2)
	column.Append(1)
	column.Append(2)
	encoded := column.AppendTo(nil)

	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{name: "empty", data: nil, err: "not enough data"},
		{name: "short header", data: encoded[:8], err: "not enough data"},
		{name: "bad magic", data: append([]byte{0}, encoded[1:]...), err: "invalid float64 column magic"},
		{
			name: "bad version",
			data: append([]byte{float64ColumnMagic, 0}, encoded[2:]...),
			err:  "unsupported float64 column format version",
		},
		{
			name: "truncated values",
			data: encoded[:len(encoded)-1],
			err:  "malformed float64 column data",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalFloat64Column(tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}
}
