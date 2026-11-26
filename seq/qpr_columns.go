package seq

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

// Column Used only for legacy engine QPR, do not use anywhere else.
// TODO(cheb0) delete this when new query engine is complete
type Column[T any] interface {
	Len() int
	Append(value T)
	AppendTo(dst []byte) []byte
	NewIter() Iter[T]
	MemUsage() int
	Compress(codecs ...Codec[T]) Column[T]
}

type Iter[T any] interface {
	Next() bool
	Value() T
}

type Codec[T any] interface {
	Compress(c Column[T]) Column[T]
	Decompress(payload []byte, rows int) (Column[T], error)
}

type columnType byte

const (
	columnTypeString columnType = iota + 1
	columnTypeInt64
	columnTypeFloat64
	columnTypeArraysFloat64
	columnTypeArraysUint32
	columnTypeSegmented
	columnTypeCompressed
)

type codecID byte

const (
	codecZstd codecID = iota + 1
	codecFrontCoding
	codecDeltaBitpack
)

const (
	columnMagic         byte = 0x43
	columnFormatVersion byte = 1
	columnHeaderSize         = 16
	columnSegmentSize        = 1024 * 1024
	columnZstdLevel          = 3
)

func compressWith[T any](c Column[T], codecs []Codec[T]) Column[T] {
	for _, codec := range codecs {
		c = codec.Compress(c)
	}
	return c
}

// compressedColumn is a column payload encoded with a codec. The payload is
// decoded only when the column is iterated.
type compressedColumn[T any] struct {
	codec   codecID
	payload []byte
	rows    int
}

func (c *compressedColumn[T]) Len() int {
	return c.rows
}

func (c *compressedColumn[T]) Append(value T) {
	panic("append to a compressed column is not supported")
}

func (c *compressedColumn[T]) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.payload)
}

func (c *compressedColumn[T]) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, columnTypeCompressed, c.rows)
	dst = append(dst, byte(c.codec))
	dst = binary.AppendUvarint(dst, uint64(len(c.payload)))
	return append(dst, c.payload...)
}

func (c *compressedColumn[T]) Compress(...Codec[T]) Column[T] {
	return c
}

func (c *compressedColumn[T]) NewIter() Iter[T] {
	col, err := c.decompress()
	if err != nil {
		panic(err)
	}
	return col.NewIter()
}

func (c *compressedColumn[T]) decompress() (Column[T], error) {
	codec, err := codecByID[T](c.codec)
	if err != nil {
		return nil, err
	}
	return codec.Decompress(c.payload, c.rows)
}

func appendColumnHeader(dst []byte, typ columnType, rows int) []byte {
	var header [columnHeaderSize]byte
	header[0] = columnMagic
	header[1] = byte(typ)
	header[2] = columnFormatVersion
	binary.LittleEndian.PutUint64(header[8:16], uint64(rows))
	return append(dst, header[:]...)
}

func parseColumnHeader(data []byte) (typ columnType, rows int, err error) {
	if len(data) < columnHeaderSize {
		return 0, 0, fmt.Errorf("not enough data for column header: %d", len(data))
	}
	if data[0] != columnMagic {
		return 0, 0, fmt.Errorf("invalid column magic byte %d", data[0])
	}
	if data[2] != columnFormatVersion {
		return 0, 0, fmt.Errorf("unsupported column format version %d", data[2])
	}
	rows64 := binary.LittleEndian.Uint64(data[8:16])
	if rows64 > uint64(math.MaxInt) {
		return 0, 0, fmt.Errorf("too many rows in column %d", rows64)
	}
	return columnType(data[1]), int(rows64), nil
}

func unpackColumn[T any](data []byte) (Column[T], error) {
	typ, rows, err := parseColumnHeader(data)
	if err != nil {
		return nil, err
	}
	body := data[columnHeaderSize:]
	switch typ {
	case columnTypeSegmented:
		return unpackSegmentedColumn[T](body, rows)
	case columnTypeCompressed:
		return unpackCompressedColumn[T](body, rows)
	case columnTypeString, columnTypeInt64, columnTypeFloat64, columnTypeArraysFloat64, columnTypeArraysUint32:
		return unpackFlatColumn[T](typ, rows, body)
	default:
		return nil, fmt.Errorf("unsupported column type %d", typ)
	}
}

func unpackCompressedColumn[T any](src []byte, rows int) (Column[T], error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("truncated compressed column codec")
	}
	codec := codecID(src[0])
	if _, err := codecByID[T](codec); err != nil {
		return nil, err
	}
	src = src[1:]
	payloadLen, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, fmt.Errorf("invalid compressed column payload length")
	}
	if uint64(len(src[n:])) < payloadLen {
		return nil, fmt.Errorf("truncated compressed column payload")
	}
	return &compressedColumn[T]{codec: codec, payload: src[n : n+int(payloadLen)], rows: rows}, nil
}

func unpackSegmentedColumn[T any](src []byte, rows int) (Column[T], error) {
	if len(src) < util.SizeOfUint32 {
		return nil, fmt.Errorf("truncated segment count")
	}
	segments := int(binary.LittleEndian.Uint32(src))
	src = src[util.SizeOfUint32:]
	column := &segmentedColumn[T]{cnt: rows, segments: make([]Column[T], 0, segments)}
	decodedRows := 0
	for i := range segments {
		blobLen, n := binary.Uvarint(src)
		if n <= 0 || uint64(len(src[n:])) < blobLen {
			return nil, fmt.Errorf("truncated segment %d", i)
		}
		segment, err := unpackColumn[T](src[n : n+int(blobLen)])
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", i, err)
		}
		src = src[n+int(blobLen):]
		decodedRows += segment.Len()
		column.segments = append(column.segments, segment)
	}
	if decodedRows != rows {
		return nil, fmt.Errorf("segment rows %d do not match column rows %d", decodedRows, rows)
	}
	return column, nil
}

func unpackFlatColumn[T any](typ columnType, rows int, payload []byte) (Column[T], error) {
	var decode func(rows int, payload []byte) (Column[T], error)
	switch typ {
	case columnTypeString:
		decode, _ = any(decodeStringColumn).(func(rows int, payload []byte) (Column[T], error))
	case columnTypeInt64:
		decode, _ = any(decodeInt64Column).(func(rows int, payload []byte) (Column[T], error))
	case columnTypeFloat64:
		decode, _ = any(decodeFloat64Column).(func(rows int, payload []byte) (Column[T], error))
	case columnTypeArraysFloat64:
		decode, _ = any(decodeArraysColumn[float64]).(func(rows int, payload []byte) (Column[T], error))
	case columnTypeArraysUint32:
		decode, _ = any(decodeArraysColumn[uint32]).(func(rows int, payload []byte) (Column[T], error))
	}
	if decode == nil {
		return nil, fmt.Errorf("column type %d does not match the column kind", typ)
	}
	return decode(rows, payload)
}

func codecByID[T any](id codecID) (Codec[T], error) {
	var codec Codec[T]
	switch id {
	case codecZstd:
		codec = zstdCodec[T]{}
	case codecFrontCoding:
		codec, _ = any(frontCoding{}).(Codec[T])
	case codecDeltaBitpack:
		codec, _ = any(deltaBitpack{}).(Codec[T])
	}
	if codec == nil {
		return nil, fmt.Errorf("column codec %d does not match the column kind", id)
	}
	return codec, nil
}

// StringColumn is the mutable string column with rows kept in a single flat
// byte slice divided by offsets.
type StringColumn struct {
	data    []byte
	offsets []uint32
}

func NewStringColumn(capacity int) *StringColumn {
	return &StringColumn{offsets: make([]uint32, 1, capacity+1)}
}

func (c *StringColumn) Len() int {
	if len(c.offsets) == 0 {
		return 0
	}
	return len(c.offsets) - 1
}

func (c *StringColumn) Append(value []byte) {
	c.data = append(c.data, value...)
	c.offsets = append(c.offsets, uint32(len(c.data)))
}

func (c *StringColumn) AppendString(value string) {
	c.data = append(c.data, value...)
	c.offsets = append(c.offsets, uint32(len(c.data)))
}

func (c *StringColumn) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.data) + util.SizeOfUint32*cap(c.offsets)
}

func (c *StringColumn) AppendTo(dst []byte) []byte {
	rows := c.Len()
	dst = appendColumnHeader(dst, columnTypeString, rows)
	if rows > 0 {
		for _, offset := range c.offsets {
			dst = binary.LittleEndian.AppendUint32(dst, offset)
		}
	}
	return append(dst, c.data...)
}

func (c *StringColumn) NewIter() Iter[[]byte] {
	return &stringColumnIter{c: c, idx: -1}
}

func (c *StringColumn) Compress(codecs ...Codec[[]byte]) Column[[]byte] {
	return compressWith(c, codecs)
}

func UnpackStringColumn(data []byte) (Column[[]byte], error) {
	return unpackColumn[[]byte](data)
}

type stringColumnIter struct {
	c   *StringColumn
	idx int
}

func (it *stringColumnIter) Next() bool {
	if it.idx+1 >= it.c.Len() {
		it.idx = it.c.Len()
		return false
	}
	it.idx++
	return true
}

func (it *stringColumnIter) Value() []byte {
	return it.c.data[it.c.offsets[it.idx]:it.c.offsets[it.idx+1]]
}

func decodeStringColumn(rows int, payload []byte) (Column[[]byte], error) {
	if rows == 0 {
		return &StringColumn{}, nil
	}
	offsetsSize := (rows + 1) * util.SizeOfUint32
	if len(payload) < offsetsSize {
		return nil, fmt.Errorf("truncated string column offsets: %d bytes", len(payload))
	}
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(payload[i*util.SizeOfUint32:])
	}
	data := payload[offsetsSize:]
	if uint64(offsets[len(offsets)-1]) != uint64(len(data)) {
		return nil, fmt.Errorf("string column offsets end at %d but data has %d bytes", offsets[len(offsets)-1], len(data))
	}
	for i := 1; i < len(offsets)-1; i++ {
		if offsets[i] > offsets[i+1] {
			return nil, fmt.Errorf("string column offsets are not monotonic at row %d", i)
		}
	}
	return &StringColumn{data: data, offsets: offsets}, nil
}

type Int64Column struct {
	values []int64
}

func NewInt64Column(capacity int) *Int64Column {
	if capacity == 0 {
		return &Int64Column{}
	}
	return &Int64Column{values: make([]int64, 0, capacity)}
}

func NewEmptyInt64Column() *Int64Column {
	return &Int64Column{}
}

func (c *Int64Column) Len() int {
	return len(c.values)
}

func (c *Int64Column) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.values)*util.SizeOfUint64
}

func (c *Int64Column) Append(v int64) {
	c.values = append(c.values, v)
}

func (c *Int64Column) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, columnTypeInt64, c.Len())
	for _, value := range c.values {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(value))
	}
	return dst
}

func (c *Int64Column) NewIter() Iter[int64] {
	return &int64ColumnIter{c: c, rowIdx: -1}
}

func (c *Int64Column) Compress(codecs ...Codec[int64]) Column[int64] {
	return compressWith(c, codecs)
}

func UnpackInt64Column(data []byte) (Column[int64], error) {
	return unpackColumn[int64](data)
}

type int64ColumnIter struct {
	c      *Int64Column
	rowIdx int
}

func (it *int64ColumnIter) Next() bool {
	if it.rowIdx+1 >= len(it.c.values) {
		it.rowIdx = len(it.c.values)
		return false
	}
	it.rowIdx++
	return true
}

func (it *int64ColumnIter) Value() int64 {
	return it.c.values[it.rowIdx]
}

func decodeInt64Column(rows int, payload []byte) (Column[int64], error) {
	if len(payload) != rows*util.SizeOfUint64 {
		return nil, fmt.Errorf("malformed int64 column data")
	}
	column := &Int64Column{}
	if rows > 0 {
		column.values = make([]int64, rows)
		for i := range column.values {
			column.values[i] = int64(binary.LittleEndian.Uint64(payload[i*util.SizeOfUint64:]))
		}
	}
	return column, nil
}

type Float64Column struct {
	values []float64
}

func NewFloat64Column(capacity int) *Float64Column {
	if capacity == 0 {
		return &Float64Column{}
	}
	return &Float64Column{values: make([]float64, 0, capacity)}
}

func (c *Float64Column) Len() int {
	return len(c.values)
}

func (c *Float64Column) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.values)*util.SizeOfFloat64
}

func (c *Float64Column) Append(value float64) {
	c.values = append(c.values, value)
}

func (c *Float64Column) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, columnTypeFloat64, c.Len())
	for _, value := range c.values {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst
}

func (c *Float64Column) NewIter() Iter[float64] {
	return &float64ColumnIter{c: c, rowIdx: -1}
}

func (c *Float64Column) Compress(codecs ...Codec[float64]) Column[float64] {
	return compressWith(c, codecs)
}

func UnpackFloat64Column(data []byte) (Column[float64], error) {
	return unpackColumn[float64](data)
}

type float64ColumnIter struct {
	c      *Float64Column
	rowIdx int
}

func (it *float64ColumnIter) Next() bool {
	if it.rowIdx+1 >= len(it.c.values) {
		it.rowIdx = len(it.c.values)
		return false
	}
	it.rowIdx++
	return true
}

func (it *float64ColumnIter) Value() float64 {
	return it.c.values[it.rowIdx]
}

func decodeFloat64Column(rows int, payload []byte) (Column[float64], error) {
	if len(payload) != rows*util.SizeOfFloat64 {
		return nil, fmt.Errorf("malformed float64 column data")
	}
	column := &Float64Column{}
	if rows > 0 {
		column.values = make([]float64, rows)
		for i := range column.values {
			column.values[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[i*util.SizeOfFloat64:]))
		}
	}
	return column, nil
}

type arrayItemType interface {
	float64 | uint32
}

// ArraysColumn is the mutable column of rows of type T with values kept in a
// single flat slice divided by offsets.
type ArraysColumn[T arrayItemType] struct {
	Values  []T
	Offsets []int
}

func NewArraysColumn[T arrayItemType](capacity int) *ArraysColumn[T] {
	return &ArraysColumn[T]{
		Values:  make([]T, 0, capacity),
		Offsets: make([]int, 1, capacity+1),
	}
}

func (c *ArraysColumn[T]) Len() int {
	if c == nil || len(c.Offsets) == 0 {
		return 0
	}
	return len(c.Offsets) - 1
}

func (c *ArraysColumn[T]) Append(values []T) {
	if len(c.Offsets) == 0 {
		c.Offsets = []int{0}
	}
	c.Values = append(c.Values, values...)
	c.Offsets = append(c.Offsets, len(c.Values))
}

func (c *ArraysColumn[T]) MemUsage() int {
	if c == nil {
		return 0
	}
	var value T
	return int(unsafe.Sizeof(*c)) + cap(c.Values)*int(unsafe.Sizeof(value)) + cap(c.Offsets)*util.SizeOfInt
}

func (c *ArraysColumn[T]) AppendTo(dst []byte) []byte {
	rows := c.Len()
	dst = appendColumnHeader(dst, arraysColumnType[T](), rows)
	dst = appendArraysValues(dst, c.Values)
	offsets := c.Offsets
	if len(offsets) == 0 {
		offsets = []int{0}
	}
	for _, offset := range offsets {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(offset))
	}
	return dst
}

func (c *ArraysColumn[T]) NewIter() Iter[[]T] {
	return &arraysColumnIter[T]{c: c, rowIdx: -1}
}

func (c *ArraysColumn[T]) Compress(codecs ...Codec[[]T]) Column[[]T] {
	return compressWith(c, codecs)
}

func UnpackArraysColumn[T arrayItemType](data []byte) (Column[[]T], error) {
	return unpackColumn[[]T](data)
}

type arraysColumnIter[T arrayItemType] struct {
	c      *ArraysColumn[T]
	rowIdx int
}

func (it *arraysColumnIter[T]) Next() bool {
	if it.rowIdx+1 >= it.c.Len() {
		it.rowIdx = it.c.Len()
		return false
	}
	it.rowIdx++
	return true
}

func (it *arraysColumnIter[T]) Value() []T {
	return it.c.Values[it.c.Offsets[it.rowIdx]:it.c.Offsets[it.rowIdx+1]]
}

func arraysColumnType[T arrayItemType]() columnType {
	var zero T
	switch any(zero).(type) {
	case float64:
		return columnTypeArraysFloat64
	default:
		return columnTypeArraysUint32
	}
}

func appendArraysValues[T arrayItemType](dst []byte, values []T) []byte {
	switch any(values).(type) {
	case []float64:
		for _, value := range any(values).([]float64) {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
		}
	case []uint32:
		for _, value := range any(values).([]uint32) {
			dst = binary.LittleEndian.AppendUint32(dst, value)
		}
	default:
		panic(fmt.Errorf("unsupported array column value type %T", values))
	}
	return dst
}

func arraysValueSize[T arrayItemType]() int {
	var zero T
	switch any(zero).(type) {
	case float64:
		return util.SizeOfFloat64
	default:
		return util.SizeOfUint32
	}
}

func decodeArraysColumn[T arrayItemType](rows int, payload []byte) (Column[[]T], error) {
	sz := arraysValueSize[T]()
	offsetsSize := (rows + 1) * util.SizeOfUint64
	if len(payload) < offsetsSize {
		return nil, fmt.Errorf("truncated array column offsets")
	}
	valuesLen := len(payload) - offsetsSize
	if valuesLen%sz != 0 {
		return nil, fmt.Errorf("malformed array column data")
	}
	values := make([]T, valuesLen/sz)
	switch typed := any(values).(type) {
	case []float64:
		for i := range typed {
			typed[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[i*util.SizeOfFloat64:]))
		}
	case []uint32:
		for i := range typed {
			typed[i] = binary.LittleEndian.Uint32(payload[i*util.SizeOfUint32:])
		}
	}
	offsets := make([]int, rows+1)
	for i := range offsets {
		offset := binary.LittleEndian.Uint64(payload[valuesLen+i*util.SizeOfUint64:])
		if offset > uint64(math.MaxInt) {
			return nil, fmt.Errorf("invalid array offset %d", offset)
		}
		offsets[i] = int(offset)
	}
	return &ArraysColumn[T]{Values: values, Offsets: offsets}, nil
}

// frontCoding a codec which stores common prefixes of consecutive strings once.
type frontCoding struct{}

func (frontCoding) Compress(c Column[[]byte]) Column[[]byte] {
	col := c.(*StringColumn)
	rows := col.Len()

	offsets := make([]uint32, 1, rows+1)
	shared := make([]uint32, rows)
	lens := make([]byte, 0, rows)
	prevStart, prevEnd := 0, 0
	for row := range rows {
		start, end := int(col.offsets[row]), int(col.offsets[row+1])
		shared[row] = uint32(commonPrefix(col.data[prevStart:prevEnd], col.data[start:end]))
		offsets = append(offsets, offsets[row]+uint32(end-start)-shared[row])
		lens = binary.AppendUvarint(lens, uint64(shared[row]))
		prevStart, prevEnd = start, end
	}

	payload := make([]byte, 0, (rows+1)*util.SizeOfUint32+int(offsets[rows])+len(lens))
	for _, offset := range offsets {
		payload = binary.LittleEndian.AppendUint32(payload, offset)
	}
	for row := range rows {
		payload = append(payload, col.data[int(col.offsets[row])+int(shared[row]):int(col.offsets[row+1])]...)
	}
	return &compressedColumn[[]byte]{codec: codecFrontCoding, payload: append(payload, lens...), rows: rows}
}

func (frontCoding) Decompress(payload []byte, rows int) (Column[[]byte], error) {
	offsetsSize := (rows + 1) * util.SizeOfUint32
	if len(payload) < offsetsSize {
		return nil, fmt.Errorf("truncated front encoded offsets")
	}
	suffixOffsets := make([]uint32, rows+1)
	for i := range suffixOffsets {
		suffixOffsets[i] = binary.LittleEndian.Uint32(payload[i*util.SizeOfUint32:])
	}
	suffix := payload[offsetsSize:]
	suffixTotal := int(suffixOffsets[rows])
	if suffixTotal > len(suffix) {
		return nil, fmt.Errorf("front encoded suffixes exceed data: %d > %d", suffixTotal, len(suffix))
	}
	lens := suffix[suffixTotal:]
	suffix = suffix[:suffixTotal]

	shared := make([]uint32, rows)
	pos := 0
	for i := range shared {
		value, n := binary.Uvarint(lens[pos:])
		if n <= 0 || value > math.MaxUint32 {
			return nil, fmt.Errorf("invalid front encoded shared length")
		}
		shared[i] = uint32(value)
		pos += n
	}
	if pos != len(lens) {
		return nil, fmt.Errorf("trailing bytes in front encoded shared lengths")
	}

	dataLen := suffixTotal
	for _, value := range shared {
		dataLen += int(value)
	}
	data := make([]byte, 0, dataLen)
	dataOffsets := make([]uint32, rows+1)
	prevStart := 0
	for row := range rows {
		start := len(data)
		keep := int(shared[row])
		if keep > start-prevStart {
			return nil, fmt.Errorf("front encoded shared length %d exceeds previous row", keep)
		}
		data = append(data, data[prevStart:prevStart+keep]...)
		data = append(data, suffix[suffixOffsets[row]:suffixOffsets[row+1]]...)
		dataOffsets[row] = uint32(start)
		prevStart = start
	}
	dataOffsets[rows] = uint32(len(data))
	return &StringColumn{data: data, offsets: dataOffsets}, nil
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type deltaBitpack struct{}

func (deltaBitpack) Compress(c Column[int64]) Column[int64] {
	col := c.(*Int64Column)
	values := make([]uint64, len(col.values))
	for i, value := range col.values {
		values[i] = uint64(value)
	}
	payload := packer.CompressDeltaBitpackUint64(nil, values, nil)
	return &compressedColumn[int64]{codec: codecDeltaBitpack, payload: payload, rows: len(values)}
}

func (deltaBitpack) Decompress(payload []byte, rows int) (Column[int64], error) {
	rest, values, err := packer.DecompressDeltaBitpackUint64(payload, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 || len(values) != rows {
		return nil, fmt.Errorf("inconsistent bitpacked values")
	}
	col := &Int64Column{values: make([]int64, len(values))}
	for i, value := range values {
		col.values[i] = int64(value)
	}
	return col, nil
}

type zstdCodec[T any] struct{}

func (zstdCodec[T]) Compress(c Column[T]) Column[T] {
	return &compressedColumn[T]{
		codec:   codecZstd,
		payload: zstd.CompressLevel(c.AppendTo(nil), nil, columnZstdLevel),
		rows:    c.Len(),
	}
}

func (zstdCodec[T]) Decompress(payload []byte, _ int) (Column[T], error) {
	blob, err := zstd.Decompress(payload, nil)
	if err != nil {
		return nil, err
	}
	return unpackColumn[T](blob)
}

// segmentedColumn is a column split into nested columns. Iteration keeps at
// most one segment iterated at a time; segments decode their payloads lazily.
type segmentedColumn[T any] struct {
	segments []Column[T]
	cnt      int
}

func (c *segmentedColumn[T]) Len() int {
	return c.cnt
}

func (c *segmentedColumn[T]) Append(value T) {
	panic("append to a segmented column is not supported")
}

func (c *segmentedColumn[T]) MemUsage() int {
	var segment Column[T]
	total := int(unsafe.Sizeof(*c)) + cap(c.segments)*int(unsafe.Sizeof(segment))
	for i := range c.segments {
		total += c.segments[i].MemUsage()
	}
	return total
}

func (c *segmentedColumn[T]) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, columnTypeSegmented, c.cnt)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(c.segments)))
	for i := range c.segments {
		blob := c.segments[i].AppendTo(nil)
		dst = binary.AppendUvarint(dst, uint64(len(blob)))
		dst = append(dst, blob...)
	}
	return dst
}

func (c *segmentedColumn[T]) Compress(codecs ...Codec[T]) Column[T] {
	segments := make([]Column[T], len(c.segments))
	for i := range c.segments {
		segments[i] = compressWith(c.segments[i], codecs)
	}
	return &segmentedColumn[T]{segments: segments, cnt: c.cnt}
}

func (c *segmentedColumn[T]) NewIter() Iter[T] {
	return &segmentedIter[T]{col: c, rowIdx: -1}
}

type segmentedIter[T any] struct {
	col    *segmentedColumn[T]
	iter   Iter[T]
	segIdx int
	rowIdx int
}

func (it *segmentedIter[T]) Next() bool {
	if it.rowIdx+1 >= it.col.cnt {
		it.rowIdx = it.col.cnt
		return false
	}
	if it.iter != nil && it.iter.Next() {
		it.rowIdx++
		return true
	}
	for it.segIdx < len(it.col.segments) {
		it.iter = it.col.segments[it.segIdx].NewIter()
		it.segIdx++
		if it.iter.Next() {
			it.rowIdx++
			return true
		}
	}
	it.rowIdx = it.col.cnt
	return false
}

func (it *segmentedIter[T]) Value() T {
	return it.iter.Value()
}

func compressSegmented[T any](c Column[T], codecs ...Codec[T]) Column[T] {
	return segmentColumn(c).Compress(codecs...)
}

func segmentColumn[T any](c Column[T]) Column[T] {
	var segments []Column[T]
	segment := newColumn[T]()
	size := 0
	it := c.NewIter()
	for it.Next() {
		value := it.Value()
		segment.Append(value)
		size += valueSize(value)
		if size >= columnSegmentSize {
			segments = append(segments, segment)
			segment = newColumn[T]()
			size = 0
		}
	}
	if segment.Len() > 0 {
		segments = append(segments, segment)
	}
	return &segmentedColumn[T]{segments: segments, cnt: c.Len()}
}

func newColumn[T any]() Column[T] {
	if c, ok := any(NewStringColumn(0)).(Column[T]); ok {
		return c
	}
	if c, ok := any(NewInt64Column(0)).(Column[T]); ok {
		return c
	}
	if c, ok := any(NewFloat64Column(0)).(Column[T]); ok {
		return c
	}
	if c, ok := any(NewArraysColumn[float64](0)).(Column[T]); ok {
		return c
	}
	if c, ok := any(NewArraysColumn[uint32](0)).(Column[T]); ok {
		return c
	}
	var zero T
	panic(fmt.Errorf("unsupported column item type %T", zero))
}

func valueSize[T any](value T) int {
	switch v := any(value).(type) {
	case []byte:
		return len(v)
	case []float64:
		return len(v) * util.SizeOfFloat64
	case []uint32:
		return len(v) * util.SizeOfUint32
	default:
		return util.SizeOfUint64
	}
}
