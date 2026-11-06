package seq

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	stringColumnMagic          byte = 0x43
	stringColumnFormatVersion  byte = 2
	stringColumnHeaderSize          = 16
	stringColumnBlockEntrySize      = 16
	stringColumnPageSize            = 1024 * 1024
	stringColumnZstdLevel           = 3
)

type stringColumnCodec byte

const (
	// stringColumnCodecFrontZstd chained encoding - front coding and zstd. For sorted tokens can yield x2-x3 better
	// compressed size comparing to using only zstd.
	stringColumnCodecFrontZstd stringColumnCodec = iota
	// stringColumnCodecZstd uses only zstd
	stringColumnCodecZstd
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

	compress(codec stringColumnCodec) StringColumn
}

type StringColumnIter interface {
	Next() bool
	Row() []byte
}

type stringColumnBlockEntry struct {
	cnt        int
	offset     int // offset of the block payload inside the column data
	offsetsLen int
	dataLen    int
	rawLen     int
}

type stringColumnBlock struct {
	data    []byte
	offsets []uint32
}

type stringColumnHeader struct {
	codec    stringColumnCodec
	cnt      int
	blocks   int
	rawBytes int
}

func NewStringColumn(capacity int) StringColumn {
	return &RawStringColumn{
		blocks: []stringColumnBlock{{offsets: make([]uint32, 1, capacity+1)}},
	}
}

// RawStringColumn is the mutable uncompressed string column
type RawStringColumn struct {
	blocks   []stringColumnBlock
	cnt      int
	rawBytes int
}

func (c *RawStringColumn) Len() int {
	return c.cnt
}

func (c *RawStringColumn) Append(value []byte) {
	page := c.openPage()
	page.data = append(page.data, value...)
	c.appendRow(page, len(value))
}

func (c *RawStringColumn) AppendString(value string) {
	page := c.openPage()
	page.data = append(page.data, value...)
	c.appendRow(page, len(value))
}

func (c *RawStringColumn) openPage() *stringColumnBlock {
	if len(c.blocks) == 0 {
		c.blocks = []stringColumnBlock{{offsets: []uint32{0}}}
		return &c.blocks[0]
	}

	page := &c.blocks[len(c.blocks)-1]
	if len(page.offsets) != 0 {
		return page
	}

	capacity := 1
	dataCapacity := 0
	if len(c.blocks) > 1 {
		previous := c.blocks[len(c.blocks)-2]
		capacity = len(previous.offsets)
		dataCapacity = min(len(previous.data), 2*stringColumnPageSize)
	}
	page.offsets = make([]uint32, 1, capacity)
	page.data = make([]byte, 0, dataCapacity)
	return page
}

func (c *RawStringColumn) appendRow(page *stringColumnBlock, size int) {
	page.offsets = append(page.offsets, uint32(len(page.data)))
	c.cnt++
	c.rawBytes += size
	if len(page.data) >= stringColumnPageSize {
		c.blocks = append(c.blocks, stringColumnBlock{})
	}
}

func (c *RawStringColumn) MemUsage() int {
	total := 0
	for i := range c.blocks {
		total += len(c.blocks[i].data) + 4*cap(c.blocks[i].offsets)
	}
	return total
}

func (c *RawStringColumn) AppendTo(dst []byte) []byte {
	return appendEncodedBlocks(dst, c.blocks, c.cnt, c.rawBytes, stringColumnCodecFrontZstd)
}

func (c *RawStringColumn) appendEncodedToWithCodec(dst []byte, codec stringColumnCodec) []byte {
	return appendEncodedBlocks(dst, c.blocks, c.cnt, c.rawBytes, codec)
}

func (c *RawStringColumn) compress(codec stringColumnCodec) StringColumn {
	encoded, err := UnmarshalStringColumn(appendEncodedBlocks(nil, c.blocks, c.cnt, c.rawBytes, codec))
	if err != nil {
		panic(fmt.Errorf("failed to compress string column: %w", err))
	}
	return encoded
}

func (c *RawStringColumn) NewIter() StringColumnIter {
	return &rawStringColumnIter{c: c, rowIdx: -1, blockIdx: -1, firstRow: -1}
}

type rawStringColumnIter struct {
	c        *RawStringColumn
	firstRow int
	rowIdx   int
	blockIdx int
}

func (it *rawStringColumnIter) Next() bool {
	if it.rowIdx+1 >= it.c.cnt {
		it.rowIdx = it.c.cnt
		return false
	}
	it.rowIdx++
	if it.blockIdx < 0 {
		it.blockIdx++
		it.firstRow = it.rowIdx
	} else if it.rowIdx >= it.firstRow+len(it.c.blocks[it.blockIdx].offsets)-1 {
		it.openNextPage()
	}
	return true
}

func (it *rawStringColumnIter) openNextPage() {
	it.blockIdx++
	it.firstRow = it.rowIdx
	for it.blockIdx < len(it.c.blocks) && len(it.c.blocks[it.blockIdx].offsets) <= 1 {
		it.blockIdx++
	}
}

func (it *rawStringColumnIter) Row() []byte {
	page := &it.c.blocks[it.blockIdx]
	inPage := it.rowIdx - it.firstRow
	return page.data[page.offsets[inPage]:page.offsets[inPage+1]]
}

func appendEncodedBlocks(dst []byte, pages []stringColumnBlock, rows, rawBytes int, codec stringColumnCodec) []byte {
	encoder := stringColumnEncoder{
		blocks: make([]stringColumnBlockEntry, 0, len(pages)),
		codec:  codec,
	}
	for i := range pages {
		if len(pages[i].offsets) <= 1 {
			continue
		}
		encoder.appendBlock(pages[i])
	}

	return encoder.appendTo(dst, rows, rawBytes)
}

type stringColumnEncoder struct {
	payload []byte
	blocks  []stringColumnBlockEntry
	codec   stringColumnCodec
	front   frontEncodeState
}

func (e *stringColumnEncoder) appendBlock(page stringColumnBlock) {
	var data []byte
	var offsets []uint32
	switch e.codec {
	case stringColumnCodecFrontZstd:
		data, offsets = frontEncode(page.data, page.offsets, &e.front)
	case stringColumnCodecZstd:
		data, offsets = page.data, page.offsets
	default:
		panic(fmt.Errorf("unsupported string column codec %d", e.codec))
	}

	offsetsStart := len(e.payload)
	e.payload = packer.CompressDeltaBitpackUint32(e.payload, offsets, nil)
	offsetsLen := len(e.payload) - offsetsStart

	dataStart := len(e.payload)
	e.payload = zstd.CompressLevel(data, e.payload, stringColumnZstdLevel)
	e.blocks = append(e.blocks, stringColumnBlockEntry{
		cnt:        len(page.offsets) - 1,
		offsetsLen: offsetsLen,
		dataLen:    len(e.payload) - dataStart,
		rawLen:     len(page.data),
	})
}

func (e *stringColumnEncoder) appendTo(dst []byte, rows, rawBytes int) []byte {
	dst = appendStringColumnHeader(dst, rows, len(e.blocks), rawBytes, e.codec)
	for i := range e.blocks {
		dst = appendStringColumnBlock(dst, &e.blocks[i])
	}
	return append(dst, e.payload...)
}

type frontEncodeState struct {
	suffix []byte
	lens   []byte
	offs   []uint32
}

func frontEncode(data []byte, offsets []uint32, s *frontEncodeState) ([]byte, []uint32) {
	rows := len(offsets) - 1
	suffix := s.suffix[:0]
	lens := s.lens[:0]
	offs := s.offs[:0]
	offs = append(offs, 0)
	for i := range rows {
		cur := data[offsets[i]:offsets[i+1]]
		shared := 0
		if i > 0 {
			prev := data[offsets[i-1]:offsets[i]]
			shared = commonPrefix(prev, cur)
		}
		suffix = append(suffix, cur[shared:]...)
		offs = append(offs, uint32(len(suffix)))
		lens = binary.AppendUvarint(lens, uint64(shared))
	}
	suffix = append(suffix, lens...)
	s.suffix = suffix
	s.lens = lens
	s.offs = offs
	return s.suffix, offs
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// CompressedStringColumn is immutable compressed string column.
// Iteration keeps only at most one block at a time uncompressed to reduce memory footprint.
type CompressedStringColumn struct {
	data   []byte                   // encoded column bytes, header included
	blocks []stringColumnBlockEntry // parsed block index pointing into data

	cnt      int
	rawBytes int
	codec    stringColumnCodec
}

func (c *CompressedStringColumn) Len() int {
	return c.cnt
}

func (c *CompressedStringColumn) Append(value []byte) {
	panic("append to a compressed string column is not supported")
}

func (c *CompressedStringColumn) AppendString(value string) {
	panic("append to a compressed string column is not supported")
}

func (c *CompressedStringColumn) MemUsage() int {
	return len(c.data) + len(c.blocks)*int(unsafe.Sizeof(stringColumnBlockEntry{}))
}

func (c *CompressedStringColumn) AppendTo(dst []byte) []byte {
	if len(c.data) == 0 {
		return appendStringColumnHeader(dst, c.cnt, len(c.blocks), c.rawBytes, c.codec)
	}
	return append(dst, c.data...)
}

func (c *CompressedStringColumn) compress(stringColumnCodec) StringColumn {
	return c
}

func (c *CompressedStringColumn) NewIter() StringColumnIter {
	return &compressedStringColumnIter{col: c, rowIdx: -1, blockIdx: -1, firstRowIdx: -1}
}

type compressedStringColumnIter struct {
	col         *CompressedStringColumn
	firstRowIdx int // first row index of the current block
	rowIdx      int // current row index
	blockIdx    int // current block
	data        []byte
	offsets     []uint32
	zbuf        []byte

	frontBuf  []byte
	frontOffs []uint32
	frontLens []uint32
}

func (it *compressedStringColumnIter) Next() bool {
	if it.rowIdx+1 >= it.col.cnt {
		it.rowIdx = it.col.cnt
		return false
	}
	it.rowIdx++
	if it.blockIdx < 0 || it.rowIdx >= it.firstRowIdx+it.col.blocks[it.blockIdx].cnt {
		it.loadBlock(it.blockIdx + 1)
	}
	return true
}

func (it *compressedStringColumnIter) Row() []byte {
	inBlock := it.rowIdx - it.firstRowIdx
	return it.data[it.offsets[inBlock]:it.offsets[inBlock+1]]
}

func (it *compressedStringColumnIter) loadBlock(block int) {
	meta := &it.col.blocks[block]
	payload := it.col.data[meta.offset : meta.offset+meta.offsetsLen+meta.dataLen]

	rest, offsets, err := packer.DecompressDeltaBitpackUint32(payload[:meta.offsetsLen], it.offsets, nil)
	if err != nil {
		panic(fmt.Errorf("failed to decode string column offsets of block %d: %w", block, err))
	}
	if len(offsets) != meta.cnt+1 || offsets[0] != 0 {
		panic(fmt.Errorf("inconsistent string column offsets in block %d", block))
	}
	it.offsets = offsets

	it.zbuf, err = zstd.Decompress(rest[:meta.dataLen], it.zbuf[:0])
	if err != nil {
		panic(fmt.Errorf("failed to decode string column data of block %d: %w", block, err))
	}

	if it.col.codec == stringColumnCodecFrontZstd {
		data, offs, err := it.frontDecode(it.zbuf, offsets)
		if err != nil {
			panic(fmt.Errorf("failed to decode front coded block %d: %w", block, err))
		}
		if int(offs[meta.cnt]) != meta.rawLen {
			panic(fmt.Errorf("inconsistent string column data length in block %d: got %d, want %d",
				block, offs[meta.cnt], meta.rawLen))
		}
		it.data, it.offsets = data, offs
	} else {
		if int(offsets[meta.cnt]) != meta.rawLen {
			panic(fmt.Errorf("inconsistent string column data length in block %d: got %d, want %d",
				block, offsets[meta.cnt], meta.rawLen))
		}
		it.data, it.offsets = it.zbuf, offsets
	}
	it.blockIdx = block
	it.firstRowIdx = it.rowIdx
}

func (it *compressedStringColumnIter) frontDecode(data []byte, offsets []uint32) ([]byte, []uint32, error) {
	rows := len(offsets) - 1
	if rows < 1 || offsets[0] != 0 {
		return nil, nil, fmt.Errorf("invalid offsets")
	}
	lens, err := it.decodeFrontLengths(data, offsets)
	if err != nil {
		return nil, nil, err
	}
	return it.rebuildFrontRows(data, offsets, lens)
}

func (it *compressedStringColumnIter) decodeFrontLengths(data []byte, offsets []uint32) ([]uint32, error) {
	rows := len(offsets) - 1
	suffixTotal := int(offsets[rows])
	if suffixTotal > len(data) {
		return nil, fmt.Errorf("suffixes exceed data: %d > %d", suffixTotal, len(data))
	}

	lens := it.frontLens[:0]
	pos := suffixTotal
	for range rows {
		value, n := binary.Uvarint(data[pos:])
		if n <= 0 || value > 1<<31 {
			return nil, fmt.Errorf("invalid shared length")
		}
		lens = append(lens, uint32(value))
		pos += n
	}
	if pos != len(data) {
		return nil, fmt.Errorf("trailing bytes in shared lengths")
	}
	it.frontLens = lens
	return lens, nil
}

func (it *compressedStringColumnIter) rebuildFrontRows(data []byte, offsets, lens []uint32) ([]byte, []uint32, error) {
	rows := len(offsets) - 1
	out := it.frontBuf[:0]
	offs := it.frontOffs[:0]
	prevStart := 0
	for i := range rows {
		start := len(out)
		if i > 0 {
			keep := int(lens[i])
			if keep > start-prevStart {
				return nil, nil, fmt.Errorf("shared length %d exceeds previous row", keep)
			}
			out = append(out, out[prevStart:prevStart+keep]...)
		} else if lens[0] != 0 {
			return nil, nil, fmt.Errorf("first row has non-zero shared length")
		}
		out = append(out, data[offsets[i]:offsets[i+1]]...)
		offs = append(offs, uint32(start))
		prevStart = start
	}
	offs = append(offs, uint32(len(out)))
	it.frontBuf = out
	it.frontOffs = offs
	return out, offs, nil
}

func UnmarshalStringColumn(data []byte) (StringColumn, error) {
	header, err := parseStringColumnHeader(data)
	if err != nil {
		return nil, err
	}
	if header.blocks == 0 {
		if header.cnt != 0 {
			return nil, fmt.Errorf("%d rows in a string column without blocks", header.cnt)
		}
		return &CompressedStringColumn{codec: header.codec}, nil
	}
	if header.cnt == 0 {
		return nil, fmt.Errorf("string column with %d blocks has no rows", header.blocks)
	}

	blocks, err := parseStringColumnBlocks(data, header.blocks)
	if err != nil {
		return nil, err
	}
	if err := validateStringColumnTotals(header, blocks); err != nil {
		return nil, err
	}
	return &CompressedStringColumn{
		data:     data,
		blocks:   blocks,
		cnt:      header.cnt,
		rawBytes: header.rawBytes,
		codec:    header.codec,
	}, nil
}

func parseStringColumnHeader(data []byte) (stringColumnHeader, error) {
	if len(data) < stringColumnHeaderSize {
		return stringColumnHeader{}, fmt.Errorf("not enough data for string column header: %d", len(data))
	}
	if data[0] != stringColumnMagic {
		return stringColumnHeader{}, fmt.Errorf("invalid string column magic byte %d", data[0])
	}
	if data[1] != stringColumnFormatVersion {
		return stringColumnHeader{}, fmt.Errorf("unsupported string column format version %d", data[1])
	}

	codec := stringColumnCodec(data[2])
	switch codec {
	case stringColumnCodecFrontZstd, stringColumnCodecZstd:
	default:
		return stringColumnHeader{}, fmt.Errorf("unsupported string column codec %d", data[2])
	}
	return stringColumnHeader{
		codec:    codec,
		cnt:      int(binary.LittleEndian.Uint32(data[4:8])),
		blocks:   int(binary.LittleEndian.Uint32(data[8:12])),
		rawBytes: int(binary.LittleEndian.Uint32(data[12:16])),
	}, nil
}

func parseStringColumnBlocks(data []byte, blocks int) ([]stringColumnBlockEntry, error) {
	indexSize := stringColumnHeaderSize + blocks*stringColumnBlockEntrySize
	if len(data) < indexSize {
		return nil, fmt.Errorf("truncated string column block index: %d bytes", len(data))
	}

	meta := make([]stringColumnBlockEntry, 0, blocks)
	offset := indexSize
	for i := range blocks {
		entry := data[stringColumnHeaderSize+i*stringColumnBlockEntrySize:]
		block := stringColumnBlockEntry{
			cnt:        int(binary.LittleEndian.Uint32(entry[0:4])),
			offsetsLen: int(binary.LittleEndian.Uint32(entry[4:8])),
			dataLen:    int(binary.LittleEndian.Uint32(entry[8:12])),
			rawLen:     int(binary.LittleEndian.Uint32(entry[12:16])),
		}
		if block.cnt == 0 {
			return nil, fmt.Errorf("string column block %d has no rows", i)
		}
		if block.offsetsLen < 4 {
			return nil, fmt.Errorf("string column block %d has invalid offsets length %d", i, block.offsetsLen)
		}
		block.offset = offset
		offset += block.offsetsLen + block.dataLen
		if offset > len(data) {
			return nil, fmt.Errorf("truncated string column block %d payload", i)
		}
		meta = append(meta, block)
	}
	return meta, nil
}

func validateStringColumnTotals(header stringColumnHeader, meta []stringColumnBlockEntry) error {
	rows := 0
	rawBytes := 0
	for i := range meta {
		rows += meta[i].cnt
		rawBytes += meta[i].rawLen
	}
	if rows != header.cnt {
		return fmt.Errorf("string column block rows %d do not match column rows %d", rows, header.cnt)
	}
	if rawBytes != header.rawBytes {
		return fmt.Errorf("string column block bytes %d do not match column bytes %d", rawBytes, header.rawBytes)
	}
	return nil
}

func appendStringColumnHeader(dst []byte, rows, blocks, rawBytes int, codec stringColumnCodec) []byte {
	var header [stringColumnHeaderSize]byte
	header[0] = stringColumnMagic
	header[1] = stringColumnFormatVersion
	header[2] = byte(codec)
	binary.LittleEndian.PutUint32(header[4:8], uint32(rows))
	binary.LittleEndian.PutUint32(header[8:12], uint32(blocks))
	binary.LittleEndian.PutUint32(header[12:16], uint32(rawBytes))
	return append(dst, header[:]...)
}

func appendStringColumnBlock(dst []byte, block *stringColumnBlockEntry) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(block.cnt))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(block.offsetsLen))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(block.dataLen))
	return binary.LittleEndian.AppendUint32(dst, uint32(block.rawLen))
}

const (
	int64ColumnMagic         byte = 0x49
	int64ColumnFormatVersion byte = 1
	int64ColumnHeaderSize         = 16
	int64ColumnEntrySize          = 16
	sizeOfInt64                   = 8
	int64ColumnBlockValues        = 128 * 1024 / 8
)

// Int64Column is a int64 column used for QPR. Used only for legacy engine QPR, do not use anywhere else.
// TODO(cheb0) delete this when query engine is complete
type Int64Column interface {
	Append(v int64)
	At(idx int) int64
	Add(row int, delta int64)
	Len() int
	MemUsage() int
	AppendTo(dst []byte) []byte
	NewIter() Int64ColumnIter

	compress() Int64Column
}

type Int64ColumnIter interface {
	Next() bool
	Value() int64
}

type int64ColumnBlockEntry struct {
	cnt     int
	offset  int
	dataLen int
	rawLen  int
}

type int64ColumnBlock struct {
	values []int64
}

type int64ColumnHeader struct {
	cnt    int
	blocks int
}

func NewInt64Column(capacity int) Int64Column {
	if capacity == 0 {
		return NewEmptyInt64Column()
	}
	pageCap := min(capacity, int64ColumnBlockValues)
	return &RawInt64Column{
		pages: []int64ColumnBlock{{values: make([]int64, 0, pageCap)}},
	}
}

func NewEmptyInt64Column() Int64Column {
	return &RawInt64Column{}
}

type RawInt64Column struct {
	pages []int64ColumnBlock
	rows  int
}

func (c *RawInt64Column) Len() int {
	return c.rows
}

func (c *RawInt64Column) MemUsage() int {
	total := len(c.pages) * int(unsafe.Sizeof(int64ColumnBlock{}))
	for i := range c.pages {
		total += 8 * cap(c.pages[i].values)
	}
	return total
}

func (c *RawInt64Column) Append(v int64) {
	if len(c.pages) == 0 {
		c.pages = []int64ColumnBlock{{}}
	}
	page := &c.pages[len(c.pages)-1]
	page.values = append(page.values, v)
	c.rows++
	if len(page.values) == int64ColumnBlockValues {
		c.pages = append(c.pages, int64ColumnBlock{})
	}
}

func (c *RawInt64Column) At(idx int) int64 {
	return c.pages[idx/int64ColumnBlockValues].values[idx%int64ColumnBlockValues]
}

func (c *RawInt64Column) Add(row int, delta int64) {
	c.pages[row/int64ColumnBlockValues].values[row%int64ColumnBlockValues] += delta
}

func (c *RawInt64Column) AppendTo(dst []byte) []byte {
	return appendEncodedInt64Pages(dst, c.pages, c.rows)
}

func (c *RawInt64Column) compress() Int64Column {
	encoded, err := UnmarshalInt64Column(appendEncodedInt64Pages(nil, c.pages, c.rows))
	if err != nil {
		panic(fmt.Errorf("failed to compress int64 column: %w", err))
	}
	return encoded
}

func (c *RawInt64Column) NewIter() Int64ColumnIter {
	return &rawInt64ColumnIter{col: c, row: -1, page: -1, firstRow: -1}
}

type rawInt64ColumnIter struct {
	col      *RawInt64Column
	firstRow int
	row      int
	page     int
}

func (it *rawInt64ColumnIter) Next() bool {
	if it.row+1 >= it.col.rows {
		it.row = it.col.rows
		return false
	}
	it.row++
	if it.page < 0 {
		it.page++
		it.firstRow = it.row
	} else if it.row >= it.firstRow+len(it.col.pages[it.page].values) {
		it.openNextPage()
	}
	return true
}

func (it *rawInt64ColumnIter) openNextPage() {
	it.page++
	it.firstRow = it.row
	for it.page < len(it.col.pages) && len(it.col.pages[it.page].values) == 0 {
		it.page++
	}
}

func (it *rawInt64ColumnIter) Value() int64 {
	return it.col.pages[it.page].values[it.row-it.firstRow]
}

func appendEncodedInt64Pages(dst []byte, pages []int64ColumnBlock, rows int) []byte {
	if rows == 0 {
		return appendInt64ColumnHeader(dst, 0, 0)
	}
	encoder := int64ColumnEncoder{
		blocks:  make([]int64ColumnBlockEntry, 0, len(pages)),
		values:  make([]uint64, 0, int64ColumnBlockValues),
		packBuf: make([]uint64, 0, int64ColumnBlockValues+5*(int64ColumnBlockValues/256)+1),
	}
	for _, page := range pages {
		if len(page.values) == 0 {
			continue
		}
		encoder.appendBlock(page)
	}
	return encoder.appendTo(dst, rows)
}

type int64ColumnEncoder struct {
	payload []byte
	blocks  []int64ColumnBlockEntry
	values  []uint64
	packBuf []uint64 // reusable buffer
}

func (e *int64ColumnEncoder) appendBlock(page int64ColumnBlock) {
	e.values = e.values[:0]
	for _, value := range page.values {
		e.values = append(e.values, uint64(value))
	}

	start := len(e.payload)
	e.payload = packer.CompressDeltaBitpackUint64(e.payload, e.values, e.packBuf[:0])
	e.blocks = append(e.blocks, int64ColumnBlockEntry{
		cnt:     len(page.values),
		dataLen: len(e.payload) - start,
		rawLen:  len(page.values) * 8,
	})
}

func (e *int64ColumnEncoder) appendTo(dst []byte, rows int) []byte {
	dst = appendInt64ColumnHeader(dst, rows, len(e.blocks))
	for i := range e.blocks {
		dst = appendInt64ColumnBlock(dst, &e.blocks[i])
	}
	return append(dst, e.payload...)
}

// DeltaBitpackedInt64Column is the compressed form of an int64 column. Iteration
// keeps only at most one block at a time uncompressed to reduce memory footprint.
type DeltaBitpackedInt64Column struct {
	data   []byte                  // encoded column bytes, header included
	blocks []int64ColumnBlockEntry // parsed block index pointing into data
	cnt    int
}

func (c *DeltaBitpackedInt64Column) Len() int {
	return c.cnt
}

func (c *DeltaBitpackedInt64Column) MemUsage() int {
	return len(c.data) + len(c.blocks)*int(unsafe.Sizeof(int64ColumnBlockEntry{}))
}

func (c *DeltaBitpackedInt64Column) Append(v int64) {
	panic("append to a compressed int64 column is not supported")
}

func (c *DeltaBitpackedInt64Column) At(idx int) int64 {
	panic("random access to a compressed int64 column is not supported")
}

func (c *DeltaBitpackedInt64Column) Add(row int, delta int64) {
	panic("update of a compressed int64 column is not supported")
}

func (c *DeltaBitpackedInt64Column) AppendTo(dst []byte) []byte {
	if len(c.data) == 0 {
		var header [int64ColumnHeaderSize]byte
		header[0] = int64ColumnMagic
		header[1] = int64ColumnFormatVersion
		binary.LittleEndian.PutUint32(header[4:8], uint32(c.cnt))
		return append(dst, header[:]...)
	}
	return append(dst, c.data...)
}

func (c *DeltaBitpackedInt64Column) compress() Int64Column {
	return c
}

func (c *DeltaBitpackedInt64Column) NewIter() Int64ColumnIter {
	return &deltaBitpackedInt64Iter{c: c, rowIdx: -1, blockIdx: -1, firstRowIdx: -1}
}

type deltaBitpackedInt64Iter struct {
	c               *DeltaBitpackedInt64Column
	rowIdx          int // current row index
	blockIdx        int // current block index
	firstRowIdx     int // index of the first row in the current block
	values          []uint64
	decompressedBuf []uint64
}

func (it *deltaBitpackedInt64Iter) Next() bool {
	if it.rowIdx+1 >= it.c.cnt {
		it.rowIdx = it.c.cnt
		return false
	}
	it.rowIdx++
	if it.blockIdx < 0 || it.rowIdx >= it.firstRowIdx+it.c.blocks[it.blockIdx].cnt {
		it.loadBlock(it.blockIdx + 1)
	}
	return true
}

func (it *deltaBitpackedInt64Iter) Value() int64 {
	return int64(it.values[it.rowIdx-it.firstRowIdx])
}

func (it *deltaBitpackedInt64Iter) loadBlock(blockIdx int) {
	meta := &it.c.blocks[blockIdx]
	payload := it.c.data[meta.offset : meta.offset+meta.dataLen]
	compressedLen := len(payload)/sizeOfInt64 + 1
	if cap(it.decompressedBuf) < compressedLen {
		it.decompressedBuf = make([]uint64, 0, compressedLen)
	}
	rest, values, err := packer.DecompressDeltaBitpackUint64(payload, it.values[:0], it.decompressedBuf[:0])
	if err != nil {
		panic(fmt.Errorf("failed to decode int64 column block %d: %w", blockIdx, err))
	}
	if len(rest) != 0 || len(values) != meta.cnt {
		panic(fmt.Errorf("inconsistent int64 column block %d", blockIdx))
	}
	it.values = values
	it.blockIdx = blockIdx
	it.firstRowIdx = it.rowIdx
}

func UnmarshalInt64Column(data []byte) (Int64Column, error) {
	header, err := parseInt64ColumnHeader(data)
	if err != nil {
		return nil, err
	}
	if header.blocks == 0 {
		if header.cnt != 0 {
			return nil, fmt.Errorf("%d values in an int64 column without blocks", header.cnt)
		}
		return &DeltaBitpackedInt64Column{}, nil
	}
	if header.cnt == 0 {
		return nil, fmt.Errorf("int64 column with %d blocks has no values", header.blocks)
	}

	meta, err := parseInt64ColumnBlocks(data, header.blocks)
	if err != nil {
		return nil, err
	}
	if err := validateInt64ColumnRows(header, meta); err != nil {
		return nil, err
	}
	return &DeltaBitpackedInt64Column{data: data, blocks: meta, cnt: header.cnt}, nil
}

func parseInt64ColumnHeader(data []byte) (int64ColumnHeader, error) {
	if len(data) < int64ColumnHeaderSize {
		return int64ColumnHeader{}, fmt.Errorf("not enough data for int64 column header: %d", len(data))
	}
	if data[0] != int64ColumnMagic {
		return int64ColumnHeader{}, fmt.Errorf("invalid int64 column magic byte %d", data[0])
	}
	if data[1] != int64ColumnFormatVersion {
		return int64ColumnHeader{}, fmt.Errorf("unsupported int64 column format version %d", data[1])
	}
	return int64ColumnHeader{
		cnt:    int(binary.LittleEndian.Uint32(data[4:8])),
		blocks: int(binary.LittleEndian.Uint32(data[8:12])),
	}, nil
}

func parseInt64ColumnBlocks(data []byte, blocks int) ([]int64ColumnBlockEntry, error) {
	indexSize := int64ColumnHeaderSize + blocks*int64ColumnEntrySize
	if len(data) < indexSize {
		return nil, fmt.Errorf("truncated int64 column block index: %d bytes", len(data))
	}

	meta := make([]int64ColumnBlockEntry, 0, blocks)
	offset := indexSize
	for i := range blocks {
		entry := data[int64ColumnHeaderSize+i*int64ColumnEntrySize:]
		block := int64ColumnBlockEntry{
			cnt:     int(binary.LittleEndian.Uint32(entry[0:4])),
			dataLen: int(binary.LittleEndian.Uint32(entry[4:8])),
			rawLen:  int(binary.LittleEndian.Uint32(entry[8:12])),
		}
		if block.cnt == 0 {
			return nil, fmt.Errorf("int64 column block %d has no values", i)
		}
		if block.rawLen != block.cnt*8 {
			return nil, fmt.Errorf("int64 column block %d has invalid raw length %d", i, block.rawLen)
		}
		block.offset = offset
		offset += block.dataLen
		if offset > len(data) {
			return nil, fmt.Errorf("truncated int64 column block %d payload", i)
		}
		meta = append(meta, block)
	}
	return meta, nil
}

func validateInt64ColumnRows(header int64ColumnHeader, meta []int64ColumnBlockEntry) error {
	total := 0
	for i := range meta {
		total += meta[i].cnt
	}
	if total != header.cnt {
		return fmt.Errorf("int64 column block values %d do not match column values %d", total, header.cnt)
	}
	return nil
}

func appendInt64ColumnHeader(dst []byte, rows, blocks int) []byte {
	var header [int64ColumnHeaderSize]byte
	header[0] = int64ColumnMagic
	header[1] = int64ColumnFormatVersion
	binary.LittleEndian.PutUint32(header[4:8], uint32(rows))
	binary.LittleEndian.PutUint32(header[8:12], uint32(blocks))
	return append(dst, header[:]...)
}

func appendInt64ColumnBlock(dst []byte, block *int64ColumnBlockEntry) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(block.cnt))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(block.dataLen))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(block.rawLen))
	return binary.LittleEndian.AppendUint32(dst, 0)
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

func (c *Float64Column) Add(row int, delta float64) {
	c.values[row] += delta
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
	return &float64ColumnIter{col: c, idx: -1}
}

type Float64ColumnIter interface {
	Next() bool
	Value() float64
}

type float64ColumnIter struct {
	col Float64Column
	idx int
}

func (it *float64ColumnIter) Next() bool {
	if it.idx+1 >= it.col.cnt {
		it.idx = it.col.cnt
		return false
	}
	it.idx++
	return true
}

func (it *float64ColumnIter) Value() float64 {
	return it.col.values[it.idx]
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
	sizeOfUint32                   = 4
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

func (c *ArraysColumn[T]) Row(row int) []T {
	start, end := c.bounds(row)
	return c.Values[start:end]
}

func (c *ArraysColumn[T]) Rows() int {
	if c == nil || c.Offsets == nil {
		return 0
	}
	return len(c.Offsets) - 1
}

func (c *ArraysColumn[T]) Append(values []T) {
	c.Values = append(c.Values, values...)
	c.Offsets = append(c.Offsets, len(c.Values))
}

func (c *ArraysColumn[T]) AppendFrom(src *ArraysColumn[T], row int, remap func([]T) []T) {
	values := src.Row(row)
	if remap != nil {
		values = remap(values)
	}
	c.Append(values)
}

func (c *ArraysColumn[T]) Replace(row int, values []T) {
	start, end := c.bounds(row)
	delta := len(values) - (end - start)
	c.Values = slices.Replace(c.Values, start, end, values...)
	for i := row + 1; i < len(c.Offsets); i++ {
		c.Offsets[i] += delta
	}
	c.Values = c.Values[:c.Offsets[len(c.Offsets)-1]]
}

func (c *ArraysColumn[T]) Validate(rows int) error {
	if c.Offsets == nil {
		if rows == 0 && len(c.Values) == 0 {
			return nil
		}
		return fmt.Errorf("invalid nil offsets")
	}
	return validateOffsets(c.Offsets, len(c.Values), rows)
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
	size := uint64(header.values)*uint64(valueSize) + uint64(header.offsets)*sizeOfInt64
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
		value := binary.LittleEndian.Uint64(data[i*sizeOfInt64:])
		if value > uint64(math.MaxInt) {
			return nil, fmt.Errorf("invalid array offset %d", value)
		}
		offsets[i] = int(value)
	}
	return offsets, nil
}

func (c *ArraysColumn[T]) bounds(row int) (int, int) {
	if c.Offsets == nil {
		return 0, 0
	}
	if row >= len(c.Offsets)-1 {
		last := c.Offsets[len(c.Offsets)-1]
		return last, last
	}
	return c.Offsets[row], c.Offsets[row+1]
}
