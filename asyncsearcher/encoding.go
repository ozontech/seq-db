package asyncsearcher

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

var be = binary.BigEndian

type qprBinVersion uint8

const (
	qprBinVersion1 qprBinVersion = iota + 1 // MIDs stored in milliseconds
	qprBinVersion2                          // MIDs stored in nanoseconds
	qprBinVersion3                          // sorted columnar QPR format
)

var availableVersions = map[qprBinVersion]struct{}{
	qprBinVersion1: {},
	qprBinVersion2: {},
	qprBinVersion3: {},
}

func marshalQPR(q *seq.QPR, dst []byte) []byte {
	return marshalQPRVersion(q, dst, qprBinVersion3)
}

func marshalQPRVersion(q *seq.QPR, dst []byte, version qprBinVersion) []byte {
	dst = append(dst, uint8(version))

	blocksLenPos := len(dst)
	dst = append(dst, make([]byte, 8)...)
	n := len(dst)
	dst = marshalIDsBlocks(dst, q.IDs)
	blocksLen := len(dst) - n
	binary.BigEndian.PutUint64(dst[blocksLenPos:], uint64(blocksLen))

	dst = marshalHistogram(dst, q.Histogram)
	dst = marshalAggs(dst, q.Aggs, version)
	dst = be.AppendUint64(dst, q.Total)
	dst = marshalErrorSource(dst, q.Errors)
	return dst
}

func unmarshalQPR(dst *seq.QPR, src []byte, idsLimit int, aggFuncs ...seq.AggFunc) (_ []byte, err error) {
	if len(src) < 19 {
		return nil, fmt.Errorf("invalid QPR format; want %d bytes, got %d", 19, len(src))
	}

	version := qprBinVersion(src[0])
	src = src[1:]
	if _, ok := availableVersions[version]; !ok {
		return nil, fmt.Errorf("invalid QPR version %d", version)
	}

	idsBlocksLen := int(be.Uint64(src))
	src = src[8:]
	if idsBlocksLen > len(src) {
		return nil, fmt.Errorf("invalid ids block length %d; want %d", len(src), idsBlocksLen)
	}
	idsBlocks := src[:idsBlocksLen]
	for i := 0; len(idsBlocks) > 0; i++ {
		if len(dst.IDs) >= idsLimit {
			break
		}
		idsBlocks, err = unmarshalIDsBlock(dst, idsBlocks, version)
		if err != nil {
			return nil, fmt.Errorf("can't unmarshal ids block at pos %d: %s", i, err)
		}
	}
	if len(dst.IDs) > idsLimit {
		dst.IDs = dst.IDs[:idsLimit]
	}
	src = src[idsBlocksLen:]

	dst.Histogram, src, err = unmarshalHistogram(src, version)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal histogram: %s", err)
	}

	dst.Aggs, src, err = unmarshalAggs(dst.Aggs, src, version, aggFuncs)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal aggs: %s", err)
	}

	dst.Total = be.Uint64(src)
	src = src[8:]

	src, dst.Errors, err = unmarshalErrorSources(dst.Errors, src)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal error sources: %s", err)
	}

	return src, nil
}

type idsCodec byte

const (
	idsCodecDelta     = 1
	idsCodecDeltaZstd = 2
)

type idsBlockHeader struct {
	Codec idsCodec
	// Length of ids block in bytes.
	Length uint32
}

func (h *idsBlockHeader) MarshalBinary(dst []byte) []byte {
	dst = append(dst, byte(h.Codec))
	dst = be.AppendUint32(dst, h.Length)
	return dst
}

func (h *idsBlockHeader) UnmarshalBinary(src []byte) ([]byte, error) {
	if len(src) < 2 {
		return src, errors.New("too few bytes")
	}
	h.Codec = idsCodec(src[0])
	src = src[1:]
	h.Length = be.Uint32(src)
	src = src[4:]
	return src, nil
}

func marshalIDsBlocks(dst []byte, ids seq.IDSources) []byte {
	b := idsBlockBufPool.Get()
	defer idsBlockBufPool.Put(b)
	const maxBlockIDsLen = 4 * 1024
	for i := 0; i < len(ids); i += maxBlockIDsLen {
		j := i + maxBlockIDsLen
		if j > len(ids) {
			j = len(ids)
		}
		blockIDs := ids[i:j]

		var codec idsCodec
		b.B, codec = marshalIDsBlock(b.B[:0], blockIDs)
		if len(b.B) > math.MaxUint32 {
			panic(fmt.Errorf("unexpected block length %d; want up to %d", len(b.B), math.MaxUint32))
		}
		header := idsBlockHeader{
			Codec:  codec,
			Length: uint32(len(b.B)),
		}
		dst = header.MarshalBinary(dst)
		dst = append(dst, b.B...)
	}
	return dst
}

var idsBlockBufPool util.BufferPool

func marshalIDsBlock(dst []byte, ids []seq.IDSource) ([]byte, idsCodec) {
	b := idsBlockBufPool.Get()
	defer idsBlockBufPool.Put(b)
	prev := seq.MID(0)
	for i := 0; i < len(ids); i++ {
		id := &ids[i]
		deltaMID := id.ID.MID - prev
		prev = id.ID.MID
		b.B = binary.AppendVarint(b.B, int64(deltaMID))
		b.B = be.AppendUint64(b.B, uint64(id.ID.RID))
		b.B = binary.AppendUvarint(b.B, id.Source)
		b.B = binary.AppendUvarint(b.B, uint64(len(id.Hint)))
		b.B = append(b.B, id.Hint...)
	}

	level := getCompressLevel(len(b.B))
	orig := dst
	dst = zstd.CompressLevel(b.B, dst, level)
	compressRatio := float64(len(dst)-len(orig)) / float64(len(b.B))
	if compressRatio < 1.05 {
		orig = append(orig, b.B...)
		return orig, idsCodecDelta
	}
	return dst, idsCodecDeltaZstd
}

func unmarshalIDsBlock(dst *seq.QPR, src []byte, version qprBinVersion) (_ []byte, err error) {
	if len(src) == 0 {
		return src, fmt.Errorf("empty IDs block")
	}
	header := idsBlockHeader{}
	src, err = header.UnmarshalBinary(src)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal ids header: %s", err)
	}

	if header.Length == 0 || len(src) < int(header.Length) {
		return nil, fmt.Errorf("unexpected IDs block length; want: %d, got: %d", header.Length, len(src))
	}
	block := src[:header.Length]
	src = src[header.Length:]
	switch header.Codec {
	case idsCodecDeltaZstd:
		b := idsBlockBufPool.Get()
		defer idsBlockBufPool.Put(b)
		b.B, err = zstd.Decompress(block, b.B)
		if err != nil {
			return src, fmt.Errorf("can't decompress ids block: %s", err)
		}
		dst.IDs, err = unmarshalIDsDelta(dst.IDs, b.B, version)
		if err != nil {
			return src, err
		}
		return src, nil
	case idsCodecDelta:
		dst.IDs, err = unmarshalIDsDelta(dst.IDs, block, version)
		if err != nil {
			return src, err
		}
		return src, nil
	default:
		return src, fmt.Errorf("unknown ids codec: %d", header.Codec)
	}
}

func unmarshalIDsDelta(dst seq.IDSources, block []byte, version qprBinVersion) (seq.IDSources, error) {
	prevMID := int64(0)
	for len(block) > 0 {
		v, n := binary.Varint(block)
		block = block[n:]
		mid := prevMID + v
		prevMID = mid

		rid := seq.RID(be.Uint64(block))
		block = block[8:]

		source, n := binary.Uvarint(block)
		block = block[n:]

		hintSize, n := binary.Uvarint(block)
		block = block[n:]
		hint := string(block[:hintSize])
		block = block[hintSize:]

		var midValue seq.MID
		if version == qprBinVersion1 {
			midValue = seq.MillisToMID(uint64(mid))
		} else {
			midValue = seq.MID(mid)
		}

		dst = append(dst, seq.IDSource{
			ID: seq.ID{
				MID: midValue,
				RID: rid,
			},
			Source: source,
			Hint:   hint,
		})
	}
	if len(block) > 0 {
		return dst, fmt.Errorf("unexpected tail when unmarshaling IDs delta")
	}
	return dst, nil
}

func marshalHistogram(dst []byte, histogram map[seq.MID]uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(histogram)))
	prev := seq.MID(0)
	for mid, hist := range histogram {
		delta := int64(mid) - int64(prev)
		prev = mid
		dst = binary.AppendVarint(dst, delta)
		dst = binary.AppendUvarint(dst, hist)
	}
	return dst
}

func unmarshalHistogram(src []byte, version qprBinVersion) (map[seq.MID]uint64, []byte, error) {
	length, n := binary.Uvarint(src)
	src = src[n:]
	if n <= 0 {
		return nil, nil, fmt.Errorf("malformed length")
	}
	dst := make(map[seq.MID]uint64, length)

	prev := int64(0)
	for i := 0; i < int(length); i++ {
		delta, n := binary.Varint(src)
		src = src[n:]
		if n <= 0 {
			return dst, src, fmt.Errorf("malformed delta mid: %d", n)
		}
		mid := delta + prev
		prev = mid

		cnt, n := binary.Uvarint(src)
		src = src[n:]
		if n <= 0 {
			return dst, src, fmt.Errorf("malformed histogram MID: %d", n)
		}

		var midValue seq.MID
		if version == qprBinVersion1 {
			midValue = seq.MillisToMID(uint64(mid))
		} else {
			midValue = seq.MID(mid)
		}

		dst[midValue] = cnt
	}
	return dst, src, nil
}

type aggsCodec byte

const (
	aggsCodecNone aggsCodec = 1
	aggsCodecZstd aggsCodec = 2
)

type aggsBlockHeader struct {
	Codec aggsCodec
	// Length if block in bytes.
	Length uint64
}

func (h *aggsBlockHeader) Marshal(dst []byte) []byte {
	dst = append(dst, byte(h.Codec))
	dst = be.AppendUint64(dst, h.Length)
	return dst
}

func (h *aggsBlockHeader) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < 9 {
		return nil, fmt.Errorf("malformed aggs header")
	}
	h.Codec = aggsCodec(src[0])
	src = src[1:]
	h.Length = be.Uint64(src)
	src = src[8:]
	return src, nil
}

var aggsCompressBufferPool util.BufferPool

func marshalAggs(dst []byte, aggs []seq.AggregatableSamples, version qprBinVersion) []byte {
	if len(aggs) == 0 {
		header := aggsBlockHeader{
			Codec:  aggsCodecNone,
			Length: 0,
		}
		dst = header.Marshal(dst)
		return dst
	}

	headerPos := len(dst)
	header := aggsBlockHeader{}
	dst = header.Marshal(dst)

	bb := aggsCompressBufferPool.Get()
	defer aggsCompressBufferPool.Put(bb)
	bb.B = bb.B[:0]
	for i := range aggs {
		bb.B = marshalAggregatableSamples(aggs[i], bb.B, version)
	}

	level := getCompressLevel(len(bb.B))
	n := len(dst)
	dst = zstd.CompressLevel(bb.B, dst, level)
	length := len(dst) - n

	header = aggsBlockHeader{
		Codec:  aggsCodecZstd,
		Length: uint64(length),
	}
	_ = header.Marshal(dst[:headerPos])

	return dst
}

func unmarshalAggs(dst []seq.AggregatableSamples, src []byte, version qprBinVersion, aggFuncs []seq.AggFunc) (_ []seq.AggregatableSamples, _ []byte, err error) {
	var header aggsBlockHeader
	src, err = header.Unmarshal(src)
	if err != nil {
		return nil, nil, err
	}

	var block []byte
	switch header.Codec {
	case aggsCodecNone:
		block = src[:header.Length]
	case aggsCodecZstd:
		bb := aggsCompressBufferPool.Get()
		defer aggsCompressBufferPool.Put(bb)
		compressedBlock := src[:header.Length]
		bb.B, err = zstd.Decompress(compressedBlock, bb.B)
		if err != nil {
			return nil, nil, err
		}
		block = bb.B
	default:
		panic(fmt.Errorf("unknown aggregation codec: %d", header.Codec))
	}
	src = src[header.Length:]

	for i := 0; len(block) > 0; i++ {
		agg := seq.AggregatableSamples{}
		var aggFunc seq.AggFunc = seq.AggFuncCount
		if i < len(aggFuncs) {
			aggFunc = aggFuncs[i]
		}
		block, err = unmarshalAggregatableSamples(&agg, block, version, aggFunc)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid QPRHistogram at pos %d: %v", i, err)
		}
		dst = append(dst, agg)
	}
	return dst, src, nil
}

func marshalAggregatableSamples(s seq.AggregatableSamples, dst []byte, version qprBinVersion) []byte {
	if version < qprBinVersion3 {
		return marshalAggregatableSamplesV1V2(s, dst)
	}
	return marshalAggregatableSamplesV3(s, dst)
}

func marshalAggregatableSamplesV3(s seq.AggregatableSamples, dst []byte) []byte {
	rows := s.RowsCount()
	dst = binary.AppendUvarint(dst, uint64(s.Values.Func))
	dst = binary.AppendUvarint(dst, uint64(len(s.Tokens)))
	dst = be.AppendUint64(dst, uint64(rows))

	for i := range s.Tokens {
		dst = appendColumn(dst, s.Tokens[i])
	}
	dst = appendPresence(dst, s.MIDs != nil)
	if s.MIDs != nil {
		dst = appendColumn(dst, s.MIDs)
	}

	values := &s.Values
	dst = appendColumn(dst, values.Totals)
	dst = appendColumn(dst, values.NotExists)
	dst = appendColumn(dst, values.Sums)
	dst = appendColumn(dst, values.Mins)
	dst = appendColumn(dst, values.Maxs)
	dst = appendColumn(dst, &values.Quantiles)
	dst = appendColumn(dst, &values.UniqueIDs)

	dst = appendPresence(dst, s.ValuesPool != nil)
	if s.ValuesPool != nil {
		dst = binary.AppendUvarint(dst, uint64(len(s.ValuesPool)))
		for _, value := range s.ValuesPool {
			dst = binary.AppendUvarint(dst, uint64(len(value)))
			dst = append(dst, value...)
		}
	}
	return be.AppendUint64(dst, uint64(s.NotExists))
}

func appendColumn(dst []byte, column interface{ AppendTo([]byte) []byte }) []byte {
	return appendBlob(dst, column.AppendTo(nil))
}

func appendBlob(dst, data []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(data)))
	return append(dst, data...)
}

func appendPresence(dst []byte, present bool) []byte {
	if present {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func marshalAggregatableSamplesV1V2(s seq.AggregatableSamples, dst []byte) []byte {
	rows := s.RowsCount()
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
		dst = marshalSamplesContainer(rowSamples(&s.Values, i), dst)
	}
	dst = be.AppendUint64(dst, uint64(s.NotExists))
	return dst
}

func unmarshalAggregatableSamples(q *seq.AggregatableSamples, src []byte, version qprBinVersion, aggFunc seq.AggFunc) ([]byte, error) {
	if len(src) < 16 {
		return nil, fmt.Errorf("src too short to unmarshal QPRHistogram, want at least 16 bytes, got %d", len(src))
	}

	if version >= qprBinVersion3 {
		return unmarshalAggregatableSamplesV3(q, src)
	}
	return unmarshalAggregatableSamplesV1V2(q, src, version, aggFunc)
}

func unmarshalAggregatableSamplesV3(q *seq.AggregatableSamples, src []byte) ([]byte, error) {
	fn, src, err := readAggFunc(src)
	if err != nil {
		return nil, err
	}
	width, src, err := readAggWidth(src)
	if err != nil {
		return nil, err
	}
	if len(src) < 8 {
		return nil, fmt.Errorf("malformed agg rows")
	}
	rows := be.Uint64(src)
	src = src[8:]
	if rows > math.MaxInt {
		return nil, fmt.Errorf("too many agg rows %d", rows)
	}

	tokens := make([]seq.StringColumn, width)
	for i := range tokens {
		data, tail, err := readBlob(src)
		if err != nil {
			return nil, fmt.Errorf("invalid token column %d: %w", i, err)
		}
		src = tail
		column, err := seq.UnmarshalStringColumn(data)
		if err != nil {
			return nil, fmt.Errorf("invalid token column %d: %w", i, err)
		}
		if column.Len() != int(rows) {
			return nil, fmt.Errorf("invalid token column %d length %d; want %d", i, column.Len(), rows)
		}
		tokens[i] = column
	}

	hasMIDs, src, err := readPresence(src)
	if err != nil {
		return nil, err
	}
	agg := seq.NewAggregatableSamplesFromColumns(fn, tokens)
	if hasMIDs {
		mids, tail, err := unmarshalInt64Column(src)
		if err != nil {
			return nil, err
		}
		src = tail
		if mids.Len() != int(rows) {
			return nil, fmt.Errorf("invalid mids column length %d; want %d", mids.Len(), rows)
		}
		agg.MIDs = mids
	}

	agg.Values, src, err = unmarshalAggValues(src, fn, int(rows))
	if err != nil {
		return nil, err
	}
	agg.ValuesPool, src, err = unmarshalValuesPool(src)
	if err != nil {
		return nil, err
	}
	if !aggregatableSamplesSorted(agg) {
		return nil, fmt.Errorf("unsorted aggregation columns")
	}
	tail, err := readAggregateNotExists(agg, src)
	if err != nil {
		return nil, err
	}
	*q = *agg
	return tail, nil
}

func readAggFunc(src []byte) (seq.AggFunc, []byte, error) {
	value, n := binary.Uvarint(src)
	if n <= 0 || value > math.MaxUint8 {
		return 0, nil, fmt.Errorf("invalid aggregation func")
	}
	return seq.AggFunc(value), src[n:], nil
}

func readAggWidth(src []byte) (int, []byte, error) {
	value, n := binary.Uvarint(src)
	if n <= 0 || value > math.MaxInt {
		return 0, nil, fmt.Errorf("invalid aggregation width")
	}
	if value > uint64(len(src)) {
		return 0, nil, fmt.Errorf("invalid aggregation width")
	}
	return int(value), src[n:], nil
}

func unmarshalInt64Column(src []byte) (seq.Int64Column, []byte, error) {
	data, tail, err := readBlob(src)
	if err != nil {
		return nil, nil, err
	}
	column, err := seq.UnmarshalInt64Column(data)
	if err != nil {
		return nil, nil, err
	}
	return column, tail, nil
}

func unmarshalFloat64Column(src []byte) (seq.Float64Column, []byte, error) {
	data, tail, err := readBlob(src)
	if err != nil {
		return seq.Float64Column{}, nil, err
	}
	column, err := seq.UnmarshalFloat64Column(data)
	if err != nil {
		return seq.Float64Column{}, nil, err
	}
	return column, tail, nil
}

func unmarshalAggValues(src []byte, fn seq.AggFunc, rows int) (seq.AggValues, []byte, error) {
	values := seq.NewAggValuesFromSamples(fn, nil)
	var err error
	values.Totals, src, err = unmarshalInt64Column(src)
	if err != nil {
		return values, nil, err
	}
	values.NotExists, src, err = unmarshalInt64Column(src)
	if err != nil {
		return values, nil, err
	}
	values.Sums, src, err = unmarshalFloat64Column(src)
	if err != nil {
		return values, nil, err
	}
	values.Mins, src, err = unmarshalFloat64Column(src)
	if err != nil {
		return values, nil, err
	}
	values.Maxs, src, err = unmarshalFloat64Column(src)
	if err != nil {
		return values, nil, err
	}
	quantileData, src, err := readBlob(src)
	if err != nil {
		return values, nil, err
	}
	values.Quantiles, err = seq.UnmarshalArraysColumn[float64](quantileData)
	if err != nil {
		return values, nil, err
	}
	uniqueIDsData, src, err := readBlob(src)
	if err != nil {
		return values, nil, err
	}
	values.UniqueIDs, err = seq.UnmarshalArraysColumn[uint32](uniqueIDsData)
	if err != nil {
		return values, nil, err
	}
	if err := values.Validate(rows); err != nil {
		return values, nil, err
	}
	return values, src, nil
}

func unmarshalValuesPool(src []byte) ([]string, []byte, error) {
	present, src, err := readPresence(src)
	if err != nil || !present {
		return nil, src, err
	}
	length, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, nil, fmt.Errorf("invalid values pool length")
	}
	if length > uint64(len(src)) {
		return nil, nil, fmt.Errorf("invalid values pool length")
	}
	src = src[n:]
	pool := make([]string, 0, length)
	for range length {
		data, tail, err := readBlob(src)
		if err != nil {
			return nil, nil, err
		}
		src = tail
		pool = append(pool, string(data))
	}
	return pool, src, nil
}

func readAggregateNotExists(agg *seq.AggregatableSamples, src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("malformed aggregate not-exists")
	}
	agg.NotExists = int64(be.Uint64(src))
	return src[8:], nil
}

func readBlob(src []byte) ([]byte, []byte, error) {
	length, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, nil, fmt.Errorf("malformed blob length")
	}
	src = src[n:]
	if uint64(len(src)) < length {
		return nil, nil, fmt.Errorf("malformed blob data")
	}
	return src[:length], src[length:], nil
}

func readPresence(src []byte) (bool, []byte, error) {
	if len(src) == 0 {
		return false, nil, fmt.Errorf("malformed presence flag")
	}
	switch src[0] {
	case 0:
		return false, src[1:], nil
	case 1:
		return true, src[1:], nil
	default:
		return false, nil, fmt.Errorf("invalid presence flag %d", src[0])
	}
}

func aggregatableSamplesSorted(agg *seq.AggregatableSamples) bool {
	iters := make([]seq.StringColumnIter, len(agg.Tokens))
	for i := range iters {
		iters[i] = agg.Tokens[i].NewIter()
		if !iters[i].Next() {
			return i == 0 && agg.RowsCount() == 0
		}
	}

	var arena []byte
	var rows [][]byte
	var mids seq.Int64ColumnIter
	if agg.MIDs != nil {
		mids = agg.MIDs.NewIter()
	}
	var prevMID seq.MID
	hasPrev := false
	for row := 0; row < agg.RowsCount(); row++ {
		mid := seq.MID(consts.DummyMID)
		if mids != nil && mids.Next() {
			mid = seq.MID(mids.Value())
		}
		if hasPrev {
			if prevMID < mid {
				return false
			}
			if prevMID == mid {
				for i := range rows {
					if bytes.Compare(rows[i], iters[i].Row()) > 0 {
						return false
					}
				}
			}
		}

		arena = arena[:0]
		rows = rows[:0]
		for i := range iters {
			row := iters[i].Row()
			start := len(arena)
			arena = append(arena, row...)
			rows = append(rows, arena[start:])
		}
		prevMID = mid
		hasPrev = true
		for i := range iters {
			iters[i].Next()
		}
	}
	return true
}

func unmarshalAggregatableSamplesV1V2(q *seq.AggregatableSamples, src []byte, version qprBinVersion, aggFunc seq.AggFunc) ([]byte, error) {
	aggs := be.Uint64(src)
	bins := make([]seq.AggBinSamples, aggs)
	src = src[8:]
	prev := int64(0)
	for i := range bins {
		delta, n := binary.Varint(src)
		if n <= 0 {
			return nil, fmt.Errorf("malformed delta mid: %d", n)
		}
		src = src[n:]
		mid := delta + prev
		prev = mid

		v, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, fmt.Errorf("invalid token size")
		}
		src = src[n:]
		if uint64(len(src)) < v {
			return nil, fmt.Errorf("invalid token data")
		}
		token := string(src[:v])
		src = src[v:]

		tail, err := unmarshalSamplesContainer(&bins[i].Samples, src)
		if err != nil {
			return nil, err
		}
		src = tail

		if version == qprBinVersion1 {
			mid = int64(seq.MillisToMID(uint64(mid)))
		}
		bins[i].Bin = seq.AggBin{MID: seq.MID(mid), Token: token}
	}
	seq.SortAggBinSamples(bins)
	*q = *seq.NewAggregatableSamplesForFunc(aggFunc, 1, len(bins))
	for i := range bins {
		q.AppendRow([]string{bins[i].Bin.Token}, bins[i].Bin.MID, bins[i].Samples)
	}

	q.NotExists = int64(be.Uint64(src))
	src = src[8:]

	return src, nil
}

func rowSamples(values *seq.AggValues, row int) *seq.SamplesContainer {
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
			samples.Samples = values.Quantiles.Row(row)
		}
	case seq.AggFuncUniqueCount:
		samples.Values = make(map[uint32]struct{}, len(values.UniqueIDs.Row(row)))
		for _, id := range values.UniqueIDs.Row(row) {
			samples.Values[id] = struct{}{}
		}
	}
	return samples
}

func marshalSamplesContainer(h *seq.SamplesContainer, dst []byte) []byte {
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

func unmarshalSamplesContainer(s *seq.SamplesContainer, src []byte) ([]byte, error) {
	if len(src) < 27 {
		return src, fmt.Errorf("histogram size too low")
	}
	s.Min = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	s.Max = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	s.Sum = math.Float64frombits(be.Uint64(src))
	src = src[8:]

	v, n := binary.Uvarint(src)
	src = src[n:]
	if n <= 0 {
		return src, fmt.Errorf("malformed total: %d", n)
	}
	s.Total = int64(v)

	v, n = binary.Uvarint(src)
	src = src[n:]
	if n <= 0 {
		return src, fmt.Errorf("malformed not_exists: %d", n)
	}
	s.NotExists = int64(v)

	samples, n := binary.Uvarint(src)
	if n <= 0 {
		return src, fmt.Errorf("malformed samples length: %d", n)
	}
	src = src[n:]
	s.Samples = slices.Grow(s.Samples[:0], int(samples))
	for i := 0; i < int(samples); i++ {
		v := math.Float64frombits(be.Uint64(src))
		s.Samples = append(s.Samples, v)
		src = src[8:]
	}
	return src, nil
}

func marshalErrorSource(dst []byte, errSrcs []seq.ErrorSource) []byte {
	dst = be.AppendUint32(dst, uint32(len(errSrcs)))
	for _, e := range errSrcs {
		dst = binary.AppendUvarint(dst, uint64(len(e.ErrStr)))
		dst = append(dst, e.ErrStr...)
		dst = binary.AppendUvarint(dst, e.Source)
	}
	return dst
}

func unmarshalErrorSources(dst []seq.ErrorSource, src []byte) ([]byte, []seq.ErrorSource, error) {
	n := be.Uint32(src)
	src = src[4:]
	if len(src) < int(n) {
		return nil, nil, fmt.Errorf("src too short to unmarshal ErrorSource")
	}
	for i := 0; i < int(n); i++ {
		length, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, nil, fmt.Errorf("malformed length of error")
		}
		src = src[n:]
		errStr := string(src[:length])
		src = src[length:]

		source, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, nil, fmt.Errorf("malformed source")
		}
		src = src[n:]

		dst = append(dst, seq.ErrorSource{
			ErrStr: errStr,
			Source: source,
		})
	}
	return src, dst, nil
}

func getCompressLevel(size int) int {
	level := 3
	if size <= 512 {
		level = 1
	} else if size <= 4*1024 {
		level = 2
	}
	return level
}
