package seq

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/util"
)

const columnCodecNone byte = 0

const (
	stringColumnMagic         byte = 0x43
	stringColumnFormatVersion byte = 1
	stringColumnHeaderSize         = 16
	sizeOfUint32                   = 4
)

// StringColumn is a string column used for QPR. Used only for legacy engine QPR, do not use anywhere else.
// TODO(cheb0) delete this when query engine is complete
type StringColumn interface {
	Len() int
	Append(value []byte)
	AppendString(value string)
	MemUsage() int
	AppendTo(dst []byte) []byte
	NewIter() StringColumnIter
}

type StringColumnIter interface {
	Next() bool
	Value() []byte
}

func NewStringColumn(capacity int) StringColumn {
	return &RawStringColumn{offsets: make([]uint32, 1, capacity+1)}
}

// RawStringColumn is the mutable raw string column.
type RawStringColumn struct {
	data    []byte
	offsets []uint32
}

func (c *RawStringColumn) Len() int {
	if len(c.offsets) == 0 {
		return 0
	}
	return len(c.offsets) - 1
}

func (c *RawStringColumn) Append(value []byte) {
	c.data = append(c.data, value...)
	c.offsets = append(c.offsets, uint32(len(c.data)))
}

func (c *RawStringColumn) AppendString(value string) {
	c.data = append(c.data, value...)
	c.offsets = append(c.offsets, uint32(len(c.data)))
}

func (c *RawStringColumn) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.data) + sizeOfUint32*cap(c.offsets)
}

func (c *RawStringColumn) AppendTo(dst []byte) []byte {
	rows := c.Len()
	dst = appendStringColumnHeader(dst, rows)
	if rows > 0 {
		for _, offset := range c.offsets {
			dst = binary.LittleEndian.AppendUint32(dst, offset)
		}
	}
	return append(dst, c.data...)
}

func (c *RawStringColumn) NewIter() StringColumnIter {
	return &rawStringColumnIter{c: c, idx: -1}
}

type rawStringColumnIter struct {
	c   *RawStringColumn
	idx int
}

func (it *rawStringColumnIter) Next() bool {
	if it.idx+1 >= it.c.Len() {
		it.idx = it.c.Len()
		return false
	}
	it.idx++
	return true
}

func (it *rawStringColumnIter) Value() []byte {
	return it.c.data[it.c.offsets[it.idx]:it.c.offsets[it.idx+1]]
}

func appendStringColumnHeader(dst []byte, rows int) []byte {
	var header [stringColumnHeaderSize]byte
	header[0] = stringColumnMagic
	header[1] = stringColumnFormatVersion
	header[2] = columnCodecNone
	binary.LittleEndian.PutUint64(header[4:12], uint64(rows))
	return append(dst, header[:]...)
}

func UnmarshalStringColumn(data []byte) (StringColumn, error) {
	if len(data) < stringColumnHeaderSize {
		return nil, fmt.Errorf("not enough data for string column header: %d", len(data))
	}
	if data[0] != stringColumnMagic {
		return nil, fmt.Errorf("invalid string column magic byte %d", data[0])
	}
	if data[1] != stringColumnFormatVersion {
		return nil, fmt.Errorf("unsupported string column format version %d", data[1])
	}
	if data[2] != columnCodecNone {
		return nil, fmt.Errorf("unsupported string column codec %d", data[2])
	}
	rows := binary.LittleEndian.Uint64(data[4:12])
	if rows > uint64(math.MaxInt) {
		return nil, fmt.Errorf("too many rows in string column %d", rows)
	}
	if rows == 0 {
		return &RawStringColumn{}, nil
	}

	offsetsSize := (int(rows) + 1) * sizeOfUint32
	if len(data) < stringColumnHeaderSize+offsetsSize {
		return nil, fmt.Errorf("truncated string column offsets: %d bytes", len(data))
	}
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(data[stringColumnHeaderSize+i*sizeOfUint32:])
	}
	columnData := data[stringColumnHeaderSize+offsetsSize:]
	if uint64(offsets[len(offsets)-1]) != uint64(len(columnData)) {
		return nil, fmt.Errorf("string column offsets end at %d but data has %d bytes", offsets[len(offsets)-1], len(columnData))
	}
	for i := 1; i < len(offsets)-1; i++ {
		if offsets[i] > offsets[i+1] {
			return nil, fmt.Errorf("string column offsets are not monotonic at row %d", i)
		}
	}
	return &RawStringColumn{data: columnData, offsets: offsets}, nil
}

const (
	int64ColumnMagic         byte = 0x49
	int64ColumnFormatVersion byte = 1
	int64ColumnHeaderSize         = 16
	sizeOfInt64                   = 8
)

// Int64Column is a int64 column used for QPR. Used only for legacy engine QPR, do not use anywhere else.
// TODO(cheb0) delete this when query engine is complete
type Int64Column interface {
	Append(v int64)
	At(idx int) int64
	Len() int
	MemUsage() int
	AppendTo(dst []byte) []byte
	NewIter() Int64ColumnIter
}

type Int64ColumnIter interface {
	Next() bool
	Value() int64
}

func NewInt64Column(capacity int) Int64Column {
	if capacity == 0 {
		return NewEmptyInt64Column()
	}
	return &RawInt64Column{values: make([]int64, 0, capacity)}
}

func NewEmptyInt64Column() Int64Column {
	return &RawInt64Column{}
}

// RawInt64Column is the mutable raw int64 column.
type RawInt64Column struct {
	values []int64
}

func (c *RawInt64Column) Len() int {
	return len(c.values)
}

func (c *RawInt64Column) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.values)*sizeOfInt64
}

func (c *RawInt64Column) Append(v int64) {
	c.values = append(c.values, v)
}

func (c *RawInt64Column) At(idx int) int64 {
	return c.values[idx]
}

func (c *RawInt64Column) AppendTo(dst []byte) []byte {
	dst = appendInt64ColumnHeader(dst, c.Len())
	for _, value := range c.values {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(value))
	}
	return dst
}

func (c *RawInt64Column) NewIter() Int64ColumnIter {
	return &rawInt64ColumnIter{c: c, row: -1}
}

type rawInt64ColumnIter struct {
	c   *RawInt64Column
	row int
}

func (it *rawInt64ColumnIter) Next() bool {
	if it.row+1 >= len(it.c.values) {
		it.row = len(it.c.values)
		return false
	}
	it.row++
	return true
}

func (it *rawInt64ColumnIter) Value() int64 {
	return it.c.values[it.row]
}

func appendInt64ColumnHeader(dst []byte, cnt int) []byte {
	var header [int64ColumnHeaderSize]byte
	header[0] = int64ColumnMagic
	header[1] = int64ColumnFormatVersion
	header[2] = columnCodecNone
	binary.LittleEndian.PutUint32(header[4:8], uint32(cnt))
	return append(dst, header[:]...)
}

func UnmarshalInt64Column(data []byte) (Int64Column, error) {
	if len(data) < int64ColumnHeaderSize {
		return nil, fmt.Errorf("not enough data for int64 column header: %d", len(data))
	}
	if data[0] != int64ColumnMagic {
		return nil, fmt.Errorf("invalid int64 column magic byte %d", data[0])
	}
	if data[1] != int64ColumnFormatVersion {
		return nil, fmt.Errorf("unsupported int64 column format version %d", data[1])
	}
	if data[2] != columnCodecNone {
		return nil, fmt.Errorf("unsupported int64 column codec %d", data[2])
	}
	cnt := int(binary.LittleEndian.Uint32(data[4:8]))
	if uint64(len(data)-int64ColumnHeaderSize) != uint64(cnt)*sizeOfInt64 {
		return nil, fmt.Errorf("malformed int64 column data")
	}
	column := &RawInt64Column{}
	if cnt > 0 {
		column.values = make([]int64, cnt)
		for i := range column.values {
			column.values[i] = int64(binary.LittleEndian.Uint64(data[int64ColumnHeaderSize+i*sizeOfInt64:]))
		}
	}
	return column, nil
}

const (
	float64ColumnMagic         byte = 0x46
	float64ColumnFormatVersion byte = 1
	float64ColumnHeaderSize         = 16
	sizeOfFloat64                   = 8
)

type Float64Column struct {
	values []float64
	cnt    int
}

func NewFloat64Column(capacity int) Float64Column {
	return Float64Column{
		values: make([]float64, 0, capacity),
	}
}

func (c Float64Column) Len() int {
	return c.cnt
}

func (c *Float64Column) Append(value float64) {
	c.values = append(c.values, value)
	c.cnt++
}

func (c *Float64Column) At(idx int) float64 {
	return c.values[idx]
}

func (c *Float64Column) Add(idx int, delta float64) {
	c.values[idx] += delta
}

func (c Float64Column) MemUsage() int {
	return cap(c.values) * sizeOfFloat64
}

func (c Float64Column) AppendTo(dst []byte) []byte {
	dst = appendFloat64ColumnHeader(dst, c.Len())
	for _, value := range c.values {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst
}

func (c Float64Column) NewIter() Float64ColumnIter {
	return &float64ColumnIter{c: c, idx: -1}
}

type Float64ColumnIter interface {
	Next() bool
	Value() float64
}

type float64ColumnIter struct {
	c   Float64Column
	idx int
}

func (it *float64ColumnIter) Next() bool {
	if it.idx+1 >= it.c.cnt {
		it.idx = it.c.cnt
		return false
	}
	it.idx++
	return true
}

func (it *float64ColumnIter) Value() float64 {
	return it.c.values[it.idx]
}

func appendFloat64ColumnHeader(dst []byte, cnt int) []byte {
	var header [float64ColumnHeaderSize]byte
	header[0] = float64ColumnMagic
	header[1] = float64ColumnFormatVersion
	binary.LittleEndian.PutUint32(header[4:8], uint32(cnt))
	return append(dst, header[:]...)
}

func UnmarshalFloat64Column(data []byte) (Float64Column, error) {
	if len(data) < float64ColumnHeaderSize {
		return Float64Column{}, fmt.Errorf("not enough data for float64 column header: %d", len(data))
	}
	if data[0] != float64ColumnMagic {
		return Float64Column{}, fmt.Errorf("invalid float64 column magic byte %d", data[0])
	}
	if data[1] != float64ColumnFormatVersion {
		return Float64Column{}, fmt.Errorf("unsupported float64 column format version %d", data[1])
	}
	cnt := int(binary.LittleEndian.Uint32(data[4:8]))
	if cnt > math.MaxInt {
		return Float64Column{}, fmt.Errorf("too many float64 column values %d", cnt)
	}
	if uint64(len(data)-float64ColumnHeaderSize) < uint64(cnt)*sizeOfFloat64 {
		return Float64Column{}, fmt.Errorf("malformed float64 column data")
	}

	column := Float64Column{
		values: make([]float64, cnt),
		cnt:    cnt,
	}
	for i := range column.values {
		offset := float64ColumnHeaderSize + i*sizeOfFloat64
		column.values[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	}
	return column, nil
}

const (
	arraysColumnMagic         byte = 0x41
	arraysColumnFormatVersion byte = 1
	arraysColumnHeaderSize         = 16
)

// ArraysColumn a column of arrays of type T. Values are kept in a single flat slice divided by offsets to reduce
// memory footprint. Used only for legacy engine QPR, do not use anywhere else.
// TODO(cheb0) delete this when query engine is complete
type ArraysColumn[T any] struct {
	Values  []T
	Offsets []int
}

func newArraysColumn[T any](capacity int) ArraysColumn[T] {
	return ArraysColumn[T]{
		Values:  make([]T, 0, capacity),
		Offsets: make([]int, 1, capacity+1),
	}
}

func (c *ArraysColumn[T]) At(idx int) []T {
	start, end := c.bounds(idx)
	return c.Values[start:end]
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
	return cap(c.Values)*int(unsafe.Sizeof(value)) + cap(c.Offsets)*util.SizeOfInt
}

func (c ArraysColumn[T]) AppendTo(dst []byte) []byte {
	dst = appendArraysColumnHeader(dst, len(c.Values), len(c.Offsets))
	switch values := any(c.Values).(type) {
	case []float64:
		for _, value := range values {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
		}
	case []uint32:
		for _, value := range values {
			dst = binary.LittleEndian.AppendUint32(dst, value)
		}
	default:
		panic(fmt.Errorf("unsupported array column value type %T", values))
	}
	for _, offset := range c.Offsets {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(offset))
	}
	return dst
}

func UnmarshalArraysColumn[T any](data []byte) (ArraysColumn[T], error) {
	var zero T
	switch any(zero).(type) {
	case float64:
		column, err := unmarshalFloat64ArrayColumn(data)
		if err != nil {
			return ArraysColumn[T]{}, err
		}
		if typed, ok := any(column).(ArraysColumn[T]); ok {
			return typed, nil
		}
	case uint32:
		column, err := unmarshalUint32ArrayColumn(data)
		if err != nil {
			return ArraysColumn[T]{}, err
		}
		if typed, ok := any(column).(ArraysColumn[T]); ok {
			return typed, nil
		}
	}
	return ArraysColumn[T]{}, fmt.Errorf("unsupported array column value type %T", zero)
}

func unmarshalFloat64ArrayColumn(data []byte) (ArraysColumn[float64], error) {
	header, err := parseArraysColumnHeader(data)
	if err != nil {
		return ArraysColumn[float64]{}, err
	}
	payload, err := arraysColumnPayload(data, header, sizeOfFloat64)
	if err != nil {
		return ArraysColumn[float64]{}, err
	}
	var values []float64
	if header.values > 0 {
		values = make([]float64, header.values)
		for i := range values {
			values[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[i*sizeOfFloat64:]))
		}
	}
	offsets, err := parseArraysColumnOffsets(payload[header.values*sizeOfFloat64:], header.offsets)
	if err != nil {
		return ArraysColumn[float64]{}, err
	}
	return ArraysColumn[float64]{Values: values, Offsets: offsets}, nil
}

func unmarshalUint32ArrayColumn(data []byte) (ArraysColumn[uint32], error) {
	header, err := parseArraysColumnHeader(data)
	if err != nil {
		return ArraysColumn[uint32]{}, err
	}
	payload, err := arraysColumnPayload(data, header, sizeOfUint32)
	if err != nil {
		return ArraysColumn[uint32]{}, err
	}
	var values []uint32
	if header.values > 0 {
		values = make([]uint32, header.values)
		for i := range values {
			values[i] = binary.LittleEndian.Uint32(payload[i*sizeOfUint32:])
		}
	}
	offsets, err := parseArraysColumnOffsets(payload[header.values*sizeOfUint32:], header.offsets)
	if err != nil {
		return ArraysColumn[uint32]{}, err
	}
	return ArraysColumn[uint32]{Values: values, Offsets: offsets}, nil
}

type arraysColumnHeader struct {
	values  int
	offsets int
}

func appendArraysColumnHeader(dst []byte, values, offsets int) []byte {
	var header [arraysColumnHeaderSize]byte
	header[0] = arraysColumnMagic
	header[1] = arraysColumnFormatVersion
	binary.LittleEndian.PutUint32(header[4:8], uint32(values))
	binary.LittleEndian.PutUint32(header[8:12], uint32(offsets))
	return append(dst, header[:]...)
}

func parseArraysColumnHeader(data []byte) (arraysColumnHeader, error) {
	if len(data) < arraysColumnHeaderSize {
		return arraysColumnHeader{}, fmt.Errorf("not enough data for array column header: %d", len(data))
	}
	if data[0] != arraysColumnMagic {
		return arraysColumnHeader{}, fmt.Errorf("invalid array column magic byte %d", data[0])
	}
	if data[1] != arraysColumnFormatVersion {
		return arraysColumnHeader{}, fmt.Errorf("unsupported array column format version %d", data[1])
	}
	return arraysColumnHeader{
		values:  int(binary.LittleEndian.Uint32(data[4:8])),
		offsets: int(binary.LittleEndian.Uint32(data[8:12])),
	}, nil
}

func arraysColumnPayload(data []byte, header arraysColumnHeader, valueSize int) ([]byte, error) {
	size := uint64(header.values)*uint64(valueSize) + uint64(header.offsets)*uint64(util.SizeOfInt64)
	if uint64(len(data)-arraysColumnHeaderSize) != size {
		return nil, fmt.Errorf("malformed array column data")
	}
	return data[arraysColumnHeaderSize:], nil
}

func parseArraysColumnOffsets(data []byte, count int) ([]int, error) {
	var offsets []int
	if count > 0 {
		offsets = make([]int, count)
	}
	for i := range offsets {
		value := binary.LittleEndian.Uint64(data[i*util.SizeOfInt64:])
		if value > uint64(math.MaxInt) {
			return nil, fmt.Errorf("invalid array offset %d", value)
		}
		offsets[i] = int(value)
	}
	return offsets, nil
}

func (c *ArraysColumn[T]) bounds(row int) (int, int) {
	if len(c.Offsets) == 0 {
		return 0, 0
	}
	if row >= len(c.Offsets)-1 {
		last := c.Offsets[len(c.Offsets)-1]
		return last, last
	}
	return c.Offsets[row], c.Offsets[row+1]
}
