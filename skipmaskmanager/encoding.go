package skipmaskmanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

type SkipMaskBinIn struct {
	LIDs []seq.LID
}

type SkipMaskBinOut struct {
	LIDs []uint32
}

type skipMaskBinVersion uint8

const (
	skipMaskBinVersion1 skipMaskBinVersion = iota + 1
)

var availableVersions = map[skipMaskBinVersion]struct{}{
	skipMaskBinVersion1: {},
}

type lidsCodec byte

const (
	lidsCodecDelta     = 1
	lidsCodecDeltaZstd = 2
)

type lidsBlockHeader struct {
	Codec  lidsCodec
	Length uint32 // Number of LIDs in block
	MinLID uint32
	MaxLID uint32
	Size   uint32 // Size of ids block in bytes.
	Offset uint64 // block's offset in file
}

func (h *lidsBlockHeader) marshal(dst []byte) {
	if len(dst) < int(lidsBlockHeaderSizeBytes) {
		panic("BUG: marshal lidsBlockHeader: len(dst) is less than header size")
	}

	dst[0] = byte(h.Codec)
	dst = dst[1:]
	binary.LittleEndian.PutUint32(dst, h.Length)
	dst = dst[sizeOfUint32:]
	binary.LittleEndian.PutUint32(dst, h.MinLID)
	dst = dst[sizeOfUint32:]
	binary.LittleEndian.PutUint32(dst, h.MaxLID)
	dst = dst[sizeOfUint32:]
	binary.LittleEndian.PutUint32(dst, h.Size)
	dst = dst[sizeOfUint32:]
	binary.LittleEndian.PutUint64(dst, h.Offset)
	dst = dst[sizeOfUint64:]
}

func (h *lidsBlockHeader) unmarshal(src []byte) ([]byte, error) {
	if len(src) < int(lidsBlockHeaderSizeBytes) {
		return src, errors.New("too few bytes")
	}

	h.Codec = lidsCodec(src[0])
	src = src[1:]
	h.Length = binary.LittleEndian.Uint32(src)
	src = src[sizeOfUint32:]
	h.MinLID = binary.LittleEndian.Uint32(src)
	src = src[sizeOfUint32:]
	h.MaxLID = binary.LittleEndian.Uint32(src)
	src = src[sizeOfUint32:]
	h.Size = binary.LittleEndian.Uint32(src)
	src = src[sizeOfUint32:]
	h.Offset = binary.LittleEndian.Uint64(src)
	src = src[sizeOfUint64:]

	return src, nil
}

func marshalSkipMask(dst []byte, in *SkipMaskBinIn) []byte {
	dst = append(dst, uint8(skipMaskBinVersion1))
	dst = marshalLIDsBlocks(dst, in.LIDs)
	return dst
}

const (
	sizeOfUint32 = unsafe.Sizeof(uint32(0))
	sizeOfUint64 = unsafe.Sizeof(uint64(0))
)

const (
	lidsBlockHeaderSizeBytes = 1 + (4 * sizeOfUint32) + sizeOfUint64
	maxLIDsBlockLen          = 1024
)

var lidsBlockBufPool util.BufferPool

func marshalLIDsBlocks(dst []byte, in []seq.LID) []byte {
	b := lidsBlockBufPool.Get()
	defer lidsBlockBufPool.Put(b)

	numberOfBlocks := (len(in) + maxLIDsBlockLen - 1) / maxLIDsBlockLen
	dst = binary.LittleEndian.AppendUint32(dst, uint32(numberOfBlocks))

	// reserve space for headers
	curHeaderOffset := len(dst)
	dst = append(dst, make([]byte, numberOfBlocks*int(lidsBlockHeaderSizeBytes))...)

	var start int
	for range numberOfBlocks {
		end := min(maxLIDsBlockLen, len(in[start:]))
		chunk := in[start : start+end]

		var codec lidsCodec
		b.B, codec = marshalLIDsBlock(b.B[:0], chunk)
		if len(b.B) > math.MaxUint32 {
			panic(fmt.Errorf("unexpected block length %d; want up to %d", len(b.B), math.MaxUint32))
		}

		header := lidsBlockHeader{
			Codec:  codec,
			Length: uint32(len(chunk)),
			MinLID: uint32(chunk[0]),
			MaxLID: uint32(chunk[len(chunk)-1]),
			Size:   uint32(len(b.B)),
			Offset: uint64(len(dst)),
		}
		header.marshal(dst[curHeaderOffset:])
		curHeaderOffset += int(lidsBlockHeaderSizeBytes)

		dst = append(dst, b.B...)
		start += end
	}

	return dst
}

func marshalLIDsBlock(dst []byte, in []seq.LID) ([]byte, lidsCodec) {
	b := lidsBlockBufPool.Get()
	defer lidsBlockBufPool.Put(b)

	prev := seq.LID(0)
	for i := range len(in) {
		lid := in[i]
		deltaLID := lid - prev
		prev = lid
		b.B = binary.AppendVarint(b.B, int64(deltaLID))
	}

	orig := dst
	dst = zstd.CompressLevel(b.B, dst, getCompressLevel(len(b.B)))

	compressRatio := float64(len(dst)-len(orig)) / float64(len(b.B))
	if compressRatio < 1.05 {
		orig = append(orig, b.B...)
		return orig, lidsCodecDelta
	}

	return dst, lidsCodecDeltaZstd
}

const minSkipMaskBytesLen = 10 // 1 byte skipMaskBinVersion + 8 byte number of LIDs + N (min 1) bytes varint + delta encoded LIDs

func unmarshalSkipMask(dst *SkipMaskBinOut, src []byte) (_ []byte, err error) {
	if len(src) < minSkipMaskBytesLen {
		return nil, fmt.Errorf("invalid skip mask format; want %d bytes, got %d", minSkipMaskBytesLen, len(src))
	}

	version := skipMaskBinVersion(src[0])
	src = src[1:]
	if _, ok := availableVersions[version]; !ok {
		return nil, fmt.Errorf("invalid skip mask binary version: %d", version)
	}

	dst.LIDs, src, err = unmarshalLIDsBlocks(dst.LIDs, src)
	if err != nil {
		return src, err
	}

	return src, nil
}

func unmarshalLIDsBlocks(dst []uint32, src []byte) ([]uint32, []byte, error) {
	numberOfBlocks := binary.LittleEndian.Uint32(src)
	src = src[sizeOfUint32:]

	var err error

	headers := make([]lidsBlockHeader, 0, numberOfBlocks)
	for range numberOfBlocks {
		header := lidsBlockHeader{}
		src, err = header.unmarshal(src)
		if err != nil {
			return dst, src, fmt.Errorf("can't unmarshal lids header: %s", err)
		}
		headers = append(headers, header)
	}

	for i := range numberOfBlocks {
		dst, src, err = unmarshalLIDsBlock(dst, src, headers[i])
		if err != nil {
			return dst, src, err
		}
	}

	if len(src) > 0 {
		return dst, src, fmt.Errorf("unexpected tail when unmarshaling LIDs blocks")
	}

	return dst, src, nil
}

func unmarshalLIDsBlock(dst []uint32, src []byte, header lidsBlockHeader) ([]uint32, []byte, error) {
	if len(src) == 0 {
		return dst, src, fmt.Errorf("empty LIDs block")
	}

	if header.Size == 0 || int(header.Size) > len(src) {
		return nil, src, fmt.Errorf("invalid LIDs block length %d; want %d", len(src), header.Size)
	}

	block := src[:header.Size]
	src = src[header.Size:]

	var err error

	switch header.Codec {
	case lidsCodecDeltaZstd:
		b := lidsBlockBufPool.Get()
		defer lidsBlockBufPool.Put(b)
		b.B, err = zstd.Decompress(block, b.B)
		if err != nil {
			return dst, src, fmt.Errorf("can't decompress ids block: %s", err)
		}
		dst, err = unmarshalLIDsDelta(dst, b.B, header)
		if err != nil {
			return dst, src, err
		}
		return dst, src, nil
	case lidsCodecDelta:
		dst, err = unmarshalLIDsDelta(dst, block, header)
		if err != nil {
			return dst, src, err
		}
		return dst, src, nil
	default:
		return dst, src, fmt.Errorf("unknown ids codec: %d", header.Codec)
	}
}

func unmarshalLIDsDelta(dst []uint32, block []byte, header lidsBlockHeader) ([]uint32, error) {
	prevLID := uint32(0)
	for range header.Length {
		v, n := binary.Varint(block)
		block = block[n:]
		lid := prevLID + uint32(v)
		prevLID = lid
		dst = append(dst, lid)
	}

	if len(block) > 0 {
		return dst, fmt.Errorf("unexpected tail when unmarshaling LIDs block")
	}

	return dst, nil
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
