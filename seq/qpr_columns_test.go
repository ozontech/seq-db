package seq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
)

func TestArraysColumnAppendTo(t *testing.T) {
	quantiles := ArraysColumn[float64]{
		Values:  []float64{1, -2},
		Offsets: []int{0, 1, 2},
	}
	quantileData := quantiles.AppendTo(nil)
	require.Len(t, quantileData, arraysColumnHeaderSize+2*sizeOfFloat64+3*sizeOfInt64)
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
	require.Len(t, uniqueIDsData, arraysColumnHeaderSize+2*sizeOfUint32+2*sizeOfInt64)
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
	require.Len(t, invalidOffset, arraysColumnHeaderSize+sizeOfFloat64+sizeOfInt64)
	binary.LittleEndian.PutUint64(invalidOffset[len(invalidOffset)-sizeOfInt64:], math.MaxUint64)
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

func requireRawStringColumn(t *testing.T, col StringColumn) *RawStringColumn {
	t.Helper()
	require.IsType(t, &RawStringColumn{}, col)
	return col.(*RawStringColumn)
}

func requireCompressedStringColumn(t *testing.T, col StringColumn) *CompressedStringColumn {
	t.Helper()
	require.IsType(t, &CompressedStringColumn{}, col)
	return col.(*CompressedStringColumn)
}

func multiBlockStringValues(blocks int) []string {
	var values []string
	total := 0
	for i := 0; total <= blocks*stringColumnPageSize; i++ {
		v := fmt.Sprintf("row-%06d-%s", i, bytes.Repeat([]byte("x"), 92))
		values = append(values, v)
		total += len(v)
	}
	return values
}

func checkStringColumnForms(t *testing.T, values []string, minBlocks int) {
	t.Helper()
	raw := buildStringColumn(t, values...)
	if minBlocks > 0 {
		require.GreaterOrEqual(t, len(requireRawStringColumn(t, raw).blocks), minBlocks, "values must span several blocks")
	}
	for _, codec := range []stringColumnCodec{stringColumnCodecFrontZstd, stringColumnCodecZstd} {
		col := raw.compress(codec)
		if minBlocks > 0 {
			require.GreaterOrEqual(t, len(requireCompressedStringColumn(t, col).blocks), minBlocks, "values must span several blocks")
		}
		requireColumnRows(t, col, values...)

		decoded, err := UnmarshalStringColumn(col.AppendTo(nil))
		require.NoError(t, err)
		requireColumnRows(t, decoded, values...)
	}
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
		name      string
		values    []string
		minBlocks int
	}{
		{"empty", nil, 0},
		{"single row", []string{"a"}, 0},
		{"empty rows", []string{"", "", ""}, 0},
		{"mixed empty", []string{"a", "", "abc", ""}, 0},
		{"duplicates", []string{"b", "a", "a", "b", "a"}, 0},
		{"unicode", []string{"héllo wörld", "привет", "日本語", ""}, 0},
		{"sorted multi block", multiBlockStringValues(3), 3},
	}
	for _, rows := range []int{1, 2, 127, 128, 129, 1000, 5000} {
		tests = append(tests, struct {
			name      string
			values    []string
			minBlocks int
		}{fmt.Sprintf("random rows=%d", rows), randomValues(rows), 0})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkStringColumnForms(t, tt.values, tt.minBlocks)
		})
	}
}

func TestStringColumnIterationStepsCompressedBlocks(t *testing.T) {
	values := multiBlockStringValues(3)
	col := buildStringColumn(t, values...).compress(stringColumnCodecZstd)
	compressed := requireCompressedStringColumn(t, col)
	require.GreaterOrEqual(t, len(compressed.blocks), 3)

	it := compressed.NewIter().(*compressedStringColumnIter)
	var transitions []int
	block := -1
	rows := 0
	for i := 0; it.Next(); i++ {
		if it.blockIdx != block {
			block = it.blockIdx
			transitions = append(transitions, i)
			require.Equal(t, compressed.blocks[block].rawLen, len(it.data), "only one block is decompressed at a time")
			require.Equal(t, compressed.blocks[block].cnt+1, len(it.offsets))
		}
		require.Equal(t, values[i], string(it.Row()))
		rows++
	}
	require.Equal(t, len(values), rows)

	want := []int{0}
	sum := 0
	for i := range compressed.blocks[:len(compressed.blocks)-1] {
		sum += compressed.blocks[i].cnt
		want = append(want, sum)
	}
	require.Equal(t, want, transitions, "blocks must be stepped through in order")
	require.Equal(t, len(compressed.blocks)-1, block)
}

func TestStringColumnIteratorsAreIndependent(t *testing.T) {
	values := multiBlockStringValues(2)
	raw := buildStringColumn(t, values...)
	forms := []struct {
		name string
		col  StringColumn
	}{
		{"raw", raw},
		{"front zstd", raw.compress(stringColumnCodecFrontZstd)},
		{"zstd", raw.compress(stringColumnCodecZstd)},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			a := form.col.NewIter()
			b := form.col.NewIter()
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
		})
	}
}

func TestStringColumnOversizedRow(t *testing.T) {
	big := bytes.Repeat([]byte("oversized!"), 10*1024) // 100KB, above the page size
	col := buildStringColumn(t, "small", string(big), "tail")
	require.Equal(t, len(big)+len("small")+len("tail"), col.(*RawStringColumn).rawBytes)
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
		{"bad codec", append([]byte{valid[0], valid[1], 0x02}, valid[3:]...), "unsupported string column codec"},
		{"truncated index", valid[:stringColumnHeaderSize+stringColumnBlockEntrySize/2], "truncated string column block index"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalStringColumn(tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}

	corrupt := append([]byte(nil), valid...)
	corrupt[4] = 42 // patch the total rows field
	_, err := UnmarshalStringColumn(corrupt)
	require.ErrorContains(t, err, "do not match column rows")

	_, err = UnmarshalStringColumn(valid[:len(valid)-1])
	require.ErrorContains(t, err, "truncated string column block")
}

func TestStringColumnCodecSelection(t *testing.T) {
	values := []string{"service-a", "service-b", "service-c"}
	col := buildStringColumn(t, values...)
	raw := requireRawStringColumn(t, col)

	frontEncoded := raw.appendEncodedToWithCodec(nil, stringColumnCodecFrontZstd)
	require.Equal(t, byte(stringColumnCodecFrontZstd), frontEncoded[2])
	frontDecoded, err := UnmarshalStringColumn(frontEncoded)
	require.NoError(t, err)
	require.Equal(t, stringColumnCodecFrontZstd, frontDecoded.(*CompressedStringColumn).codec)
	requireColumnRows(t, frontDecoded, values...)

	zstdEncoded := raw.appendEncodedToWithCodec(nil, stringColumnCodecZstd)
	require.Equal(t, byte(stringColumnCodecZstd), zstdEncoded[2])
	zstdDecoded, err := UnmarshalStringColumn(zstdEncoded)
	require.NoError(t, err)
	require.Equal(t, stringColumnCodecZstd, zstdDecoded.(*CompressedStringColumn).codec)
	requireColumnRows(t, zstdDecoded, values...)
}

func TestUnmarshalStringColumnAliasesInput(t *testing.T) {
	encoded := buildStringColumn(t, "aaa", "bbbb").AppendTo(nil)
	decoded, err := UnmarshalStringColumn(encoded)
	require.NoError(t, err)
	require.Same(t, &encoded[0], &decoded.(*CompressedStringColumn).data[0])
}

func TestStringColumnModes(t *testing.T) {
	values := make([]string, 2000)
	for i := range values {
		values[i] = fmt.Sprintf("service-%04d/pod-%04d", i%97, i)
	}

	col := NewStringColumn(len(values))
	for _, v := range values {
		col.AppendString(v)
	}
	requireRawStringColumn(t, col)
	requireColumnRows(t, col, values...)

	encodedWhileRaw := col.AppendTo(nil)
	requireRawStringColumn(t, col)
	requireColumnRows(t, col, values...)

	it := col.NewIter()
	require.True(t, it.Next())
	first := it.Row()
	require.True(t, it.Next())
	require.Equal(t, values[0], string(first))

	col = col.compress(stringColumnCodecFrontZstd)
	requireCompressedStringColumn(t, col)
	requireColumnRows(t, col, values...)
	require.Panics(t, func() { col.AppendString("compressed") })
	require.Equal(t, encodedWhileRaw, col.AppendTo(nil), "encoding must not depend on the column mode")

	// compress is idempotent
	before := col.AppendTo(nil)
	col = col.compress(stringColumnCodecFrontZstd)
	require.Equal(t, before, col.AppendTo(nil))
}

func TestQPRToCompressed(t *testing.T) {
	agg := NewAggregatableSamplesForFunc(AggFuncCount, 1, 0)
	for i := 0; i < 100; i++ {
		agg.AppendRow([]string{fmt.Sprintf("token-%03d", i)}, consts.DummyMID, SamplesContainer{Total: 1})
	}
	qpr := &QPR{Aggs: []AggregatableSamples{*agg}}

	requireRawStringColumn(t, qpr.Aggs[0].Tokens[0])
	encoded := qpr.Aggs[0].Tokens[0].AppendTo(nil)

	qpr.ToCompressed()
	requireCompressedStringColumn(t, qpr.Aggs[0].Tokens[0])
	require.Equal(t, encoded, qpr.Aggs[0].Tokens[0].AppendTo(nil))

	values := make([]string, 100)
	for i := range values {
		values[i] = fmt.Sprintf("token-%03d", i)
	}
	requireColumnRows(t, qpr.Aggs[0].Tokens[0], values...)

	// idempotent, and safe on QPRs without aggs
	qpr.ToCompressed()
	empty := &QPR{}
	empty.ToCompressed()
}

func TestQPRToCompressedSelectsStringColumnCodec(t *testing.T) {
	newAgg := func(mid MID) AggregatableSamples {
		agg := NewAggregatableSamplesForFunc(AggFuncCount, 1, 3)
		for i := range 3 {
			agg.AppendRow([]string{fmt.Sprintf("service-%d", i)}, mid, SamplesContainer{Total: 1})
		}
		agg.SortRows()
		return *agg
	}

	ordinary := &QPR{Aggs: []AggregatableSamples{newAgg(consts.DummyMID)}}
	ordinary.ToCompressed()
	encoded := ordinary.Aggs[0].Tokens[0].AppendTo(nil)
	require.Equal(t, byte(stringColumnCodecFrontZstd), encoded[2])
	decoded, err := UnmarshalStringColumn(encoded)
	require.NoError(t, err)
	require.Equal(t, stringColumnCodecFrontZstd, decoded.(*CompressedStringColumn).codec)

	timeSeries := &QPR{Aggs: []AggregatableSamples{newAgg(MID(42))}}
	timeSeries.ToCompressed()
	require.IsType(t, &DeltaBitpackedInt64Column{}, timeSeries.Aggs[0].MIDs)
	encoded = timeSeries.Aggs[0].Tokens[0].AppendTo(nil)
	require.Equal(t, byte(stringColumnCodecZstd), encoded[2])
	decoded, err = UnmarshalStringColumn(encoded)
	require.NoError(t, err)
	require.Equal(t, stringColumnCodecZstd, decoded.(*CompressedStringColumn).codec)
	requireColumnRows(t, decoded, "service-0", "service-1", "service-2")
}

func TestQPRToCompressedFrontCodesOnlyFirstTokenColumn(t *testing.T) {
	agg := NewAggregatableSamplesForFunc(AggFuncCount, 2, 2)
	agg.AppendRow([]string{"a", "z"}, consts.DummyMID, SamplesContainer{Total: 1})
	agg.AppendRow([]string{"b", "a"}, consts.DummyMID, SamplesContainer{Total: 1})
	agg.SortRows()
	qpr := &QPR{Aggs: []AggregatableSamples{*agg}}

	qpr.ToCompressed()

	for i, want := range []stringColumnCodec{stringColumnCodecFrontZstd, stringColumnCodecZstd} {
		encoded := qpr.Aggs[0].Tokens[i].AppendTo(nil)
		require.Equal(t, byte(want), encoded[2])
		decoded, err := UnmarshalStringColumn(encoded)
		require.NoError(t, err)
		require.Equal(t, want, decoded.(*CompressedStringColumn).codec)
	}
}

func TestStringColumnCompressionRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("compression report is skipped in short mode")
	}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xFF, (i>>8)&0xFF, i&0xFF)
	}
	slices.Sort(values)

	col := NewStringColumn(len(values))
	for _, v := range values {
		col.AppendString(v)
	}

	raw := 0
	for _, v := range values {
		raw += len(v)
	}
	encoded := col.AppendTo(nil)
	t.Logf("rows=%d raw=%d encoded=%d pages=%d ratio=%.2f",
		len(values), raw, len(encoded), len(col.(*RawStringColumn).blocks), float64(raw)/float64(len(encoded)))
	require.Less(t, len(encoded), raw)
}

func TestStringColumnFrontCoding(t *testing.T) {
	t.Run("sorted rows are front coded", func(t *testing.T) {
		values := make([]string, 2000)
		for i := range values {
			values[i] = fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xFF, (i>>8)&0xFF, i&0xFF)
		}
		slices.Sort(values)

		col := buildStringColumn(t, values...)
		col = col.compress(stringColumnCodecFrontZstd)
		compressed := requireCompressedStringColumn(t, col)
		require.NotEmpty(t, compressed.blocks)
		require.Equal(t, stringColumnCodecFrontZstd, compressed.codec)
		requireColumnRows(t, col, values...)
	})

	t.Run("unshared rows use zstd-only codec", func(t *testing.T) {
		rng := rand.New(rand.NewSource(11))
		col := NewStringColumn(0)
		var values []string
		for i := 0; i < 2000; i++ {
			buf := make([]byte, 40)
			rng.Read(buf)
			values = append(values, string(buf))
			col.Append(buf)
		}
		col = col.compress(stringColumnCodecZstd)
		compressed := requireCompressedStringColumn(t, col)
		require.Equal(t, stringColumnCodecZstd, compressed.codec)
		requireColumnRows(t, col, values...)
	})
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

func checkInt64ColumnForms(t *testing.T, values []int64, minBlocks int) {
	t.Helper()
	raw := NewInt64Column(len(values))
	for _, v := range values {
		raw.Append(v)
	}
	requireInt64Values(t, raw, values...)
	if minBlocks > 0 {
		require.GreaterOrEqual(t, len(raw.(*RawInt64Column).pages), minBlocks, "values must span several pages")
	}

	col := raw.compress()
	if minBlocks > 0 {
		require.GreaterOrEqual(t, len(col.(*DeltaBitpackedInt64Column).blocks), minBlocks, "values must span several blocks")
	}
	requireInt64Values(t, col, values...)

	decoded, err := UnmarshalInt64Column(col.AppendTo(nil))
	require.NoError(t, err)
	requireInt64Values(t, decoded, values...)
}

func TestNewEmptyInt64Column(t *testing.T) {
	column := NewEmptyInt64Column()
	require.IsType(t, &RawInt64Column{}, column)
	raw := column.(*RawInt64Column)
	require.Empty(t, raw.pages)
	require.Zero(t, column.Len())
	require.Zero(t, column.MemUsage())
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

func TestInt64ColumnIteration(t *testing.T) {
	for _, rows := range []int{
		0, 1, 2, 127, 128, 129, 1000,
		int64ColumnBlockValues - 1,
		int64ColumnBlockValues,
		int64ColumnBlockValues + 1,
		2*int64ColumnBlockValues + 5,
		3*int64ColumnBlockValues + 7,
	} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			minBlocks := 0
			if rows > 2*int64ColumnBlockValues {
				minBlocks = 3
			}
			checkInt64ColumnForms(t, genInt64Values(rows), minBlocks)
		})
	}
}

func TestInt64ColumnIterationStepsBlocks(t *testing.T) {
	values := genInt64Values(3*int64ColumnBlockValues + 7)
	raw := NewInt64Column(len(values))
	for _, v := range values {
		raw.Append(v)
	}
	col := raw.compress()
	compressed := col.(*DeltaBitpackedInt64Column)
	require.GreaterOrEqual(t, len(compressed.blocks), 3)

	it := compressed.NewIter().(*deltaBitpackedInt64Iter)
	var transitions []int
	block := -1
	for i := 0; it.Next(); i++ {
		if it.blockIdx != block {
			block = it.blockIdx
			transitions = append(transitions, i)
			require.Equal(t, compressed.blocks[block].cnt, len(it.values), "only one block is decompressed at a time")
		}
		require.Equal(t, values[i], it.Value())
	}

	want := []int{0}
	sum := 0
	for i := range compressed.blocks[:len(compressed.blocks)-1] {
		sum += compressed.blocks[i].cnt
		want = append(want, sum)
	}
	require.Equal(t, want, transitions, "blocks must be stepped through in order")
	require.Equal(t, len(compressed.blocks)-1, block)
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
		{"page boundary", genInt64Values(int64ColumnBlockValues)},
		{"page boundary plus one", genInt64Values(int64ColumnBlockValues + 1)},
		{"multiple pages", genInt64Values(5*int64ColumnBlockValues + 17)},
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

			expected := slices.Clone(tt.values)
			if len(expected) > 0 {
				col.Add(0, 7)
				expected[0] += 7
			}
			requireInt64Values(t, col, expected...)
		})
	}
}

func TestInt64ColumnModes(t *testing.T) {
	values := genInt64Values(2*int64ColumnBlockValues + 3)

	col := NewInt64Column(len(values))
	for _, v := range values {
		col.Append(v)
	}
	encodedWhileRaw := col.AppendTo(nil)

	col = col.compress()
	requireInt64Values(t, col, values...)
	require.Equal(t, encodedWhileRaw, col.AppendTo(nil), "encoding must not depend on the column mode")

	// compress is idempotent
	col = col.compress()
	require.Equal(t, encodedWhileRaw, col.AppendTo(nil))
}

func TestInt64ColumnCompressedAccessPanics(t *testing.T) {
	col := NewInt64Column(1)
	col.Append(1)
	col = col.compress()
	require.Panics(t, func() { col.Append(2) })
	require.Panics(t, func() { col.At(0) })
	require.Panics(t, func() { col.Add(0, 1) })
}

func TestInt64ColumnAbsent(t *testing.T) {
	var col Int64Column
	require.Nil(t, col)
	agg := &AggregatableSamples{MIDs: col, Values: newAggValues(AggFuncUnique, 0)}
	require.Zero(t, agg.RowsCount())
	require.Zero(t, agg.Values.memUsage())
	qpr := &QPR{Aggs: []AggregatableSamples{*agg}}
	qpr.ToCompressed()
	require.Nil(t, agg.MIDs)
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

	corrupt := append([]byte(nil), valid...)
	corrupt[4] = 42 // patch the values count
	_, err = UnmarshalInt64Column(corrupt)
	require.ErrorContains(t, err, "do not match column values")

	_, err = UnmarshalInt64Column(valid[:len(valid)-1])
	require.ErrorContains(t, err, "truncated int64 column block")
}

func TestInt64ColumnCompressionRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("compression report is skipped in short mode")
	}
	rng := rand.New(rand.NewSource(7))
	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(1 + rng.Intn(1<<14)) // counter-like totals
	}
	col := NewInt64Column(len(values))
	for _, v := range values {
		col.Append(v)
	}
	encoded := col.AppendTo(nil)
	t.Logf("values=%d raw=%d encoded=%d ratio=%.2f", len(values), 8*len(values), len(encoded),
		float64(8*len(values))/float64(len(encoded)))
	require.Less(t, len(encoded), 8*len(values))
}

func TestInt64ColumnDescendingMIDsCompressionRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("compression report is skipped in short mode")
	}
	const rows = 100_000
	col := NewInt64Column(rows)
	for i := range rows {
		bucket := (rows - 1 - i) / 1000
		col.Append(int64(MID(uint64(bucket) * uint64(time.Minute))))
	}

	encoded := col.AppendTo(nil)
	t.Logf("values=%d raw=%d encoded=%d ratio=%.2f", rows, 8*rows, len(encoded),
		float64(8*rows)/float64(len(encoded)))
	require.Less(t, len(encoded), 8*rows/20)
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
