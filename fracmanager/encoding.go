package fracmanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

var be = binary.BigEndian

const qprBinVersion = uint8(1)

func marshalQPR(q *seq.QPR, dst []byte) []byte {
	dst = append(dst, qprBinVersion)

	blocksLenPos := len(dst)
	dst = append(dst, make([]byte, 8)...)
	n := len(dst)
	dst = marshalIDsBlocks(dst, q.IDs)
	blocksLen := len(dst) - n
	binary.BigEndian.PutUint64(dst[blocksLenPos:], uint64(blocksLen))

	dst = marshalHistogram(dst, q.Histogram)
	dst = marshalAggs(dst, q.Aggs)
	dst = be.AppendUint64(dst, q.Total)
	dst = marshalErrorSource(dst, q.Errors)
	return dst
}

func unmarshalQPR(dst *seq.QPR, src []byte, idsLimit int) (_ []byte, err error) {
	if len(src) < 19 {
		return nil, fmt.Errorf("invalid QPR format; want %d bytes, got %d", 41, len(src))
	}

	version := src[0]
	src = src[1:]
	if version != qprBinVersion {
		return nil, fmt.Errorf("invalid QPR version %d; want %d", version, qprBinVersion)
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
		idsBlocks, err = unmarshalIDsBlock(dst, idsBlocks)
		if err != nil {
			return nil, fmt.Errorf("can't unmarshal ids block at pos %d: %s", i, err)
		}
	}
	if len(dst.IDs) > idsLimit {
		dst.IDs = dst.IDs[:idsLimit]
	}
	src = src[idsBlocksLen:]

	dst.Histogram, src, err = unmarshalHistogram(src)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal histogram: %s", err)
	}

	dst.Aggs, src, err = unmarshalAggs(dst.Aggs, src)
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
			panic(fmt.Errorf("unexpected block length %d; want up to %d", len(b.B), math.MaxUint16))
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

func unmarshalIDsBlock(dst *seq.QPR, src []byte) (_ []byte, err error) {
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
		dst.IDs, err = unmarshalIDsDelta(dst.IDs, b.B)
		if err != nil {
			return src, err
		}
		return src, nil
	case idsCodecDelta:
		dst.IDs, err = unmarshalIDsDelta(dst.IDs, block)
		if err != nil {
			return src, err
		}
		return src, nil
	default:
		return src, fmt.Errorf("unknown ids codec: %d", header.Codec)
	}
}

func unmarshalIDsDelta(dst seq.IDSources, block []byte) (seq.IDSources, error) {
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

		dst = append(dst, seq.IDSource{
			ID: seq.ID{
				MID: seq.MID(mid),
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

func unmarshalHistogram(src []byte) (map[seq.MID]uint64, []byte, error) {
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

		dst[seq.MID(mid)] = cnt
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

func marshalAggs(dst []byte, aggs []seq.QPRHistogram) []byte {
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
	for _, agg := range aggs {
		bb.B = marshalQPRHistogram(agg, bb.B)
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

func unmarshalAggs(dst []seq.QPRHistogram, src []byte) (_ []seq.QPRHistogram, _ []byte, err error) {
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
		agg := seq.QPRHistogram{}
		block, err = unmarshalQPRHistogram(&agg, block)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid QPRHistogram at pos %d: %v", i, err)
		}
		dst = append(dst, agg)
	}
	return dst, src, nil
}

func marshalQPRHistogram(q seq.QPRHistogram, dst []byte) []byte {
	dst = be.AppendUint64(dst, uint64(len(q.HistogramByToken)))
	for token, hist := range q.HistogramByToken {
		dst = binary.AppendUvarint(dst, uint64(len(token)))
		dst = append(dst, token...)
		dst = marshalAggregationHistogram(hist, dst)
	}
	dst = be.AppendUint64(dst, uint64(q.NotExists))
	return dst
}

func unmarshalQPRHistogram(q *seq.QPRHistogram, src []byte) ([]byte, error) {
	if len(src) < 16 {
		return nil, fmt.Errorf("src too short to unmarshal QPRHistogram, want at least 16 bytes, got %d", len(src))
	}

	aggs := be.Uint64(src)
	src = src[8:]
	q.HistogramByToken = make(map[string]*seq.AggregationHistogram, aggs)
	hists := make([]seq.AggregationHistogram, aggs)
	for i := 0; i < int(aggs); i++ {
		v, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, fmt.Errorf("invalid token size")
		}
		src = src[n:]
		token := string(src[:v])
		src = src[v:]

		hist := &hists[i]
		tail, err := unmarshalAggregationHistogram(hist, src)
		if err != nil {
			return nil, err
		}
		src = tail

		q.HistogramByToken[token] = hist
	}

	q.NotExists = int64(be.Uint64(src))
	src = src[8:]

	return src, nil
}

func marshalAggregationHistogram(h *seq.AggregationHistogram, dst []byte) []byte {
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

func unmarshalAggregationHistogram(h *seq.AggregationHistogram, src []byte) ([]byte, error) {
	if len(src) < 27 {
		return src, fmt.Errorf("histogram size too low")
	}
	h.Min = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	h.Max = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	h.Sum = math.Float64frombits(be.Uint64(src))
	src = src[8:]

	v, n := binary.Uvarint(src)
	src = src[n:]
	if n <= 0 {
		return src, fmt.Errorf("malformed total: %d", n)
	}
	h.Total = int64(v)

	v, n = binary.Uvarint(src)
	src = src[n:]
	if n <= 0 {
		return src, fmt.Errorf("malformed not_exists: %d", n)
	}
	h.NotExists = int64(v)

	samples, n := binary.Uvarint(src)
	if n <= 0 {
		return src, fmt.Errorf("malformed samples length: %d", n)
	}
	src = src[n:]
	h.Samples = slices.Grow(h.Samples[:0], int(samples))
	for i := 0; i < int(samples); i++ {
		v := math.Float64frombits(be.Uint64(src))
		h.Samples = append(h.Samples, v)
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
