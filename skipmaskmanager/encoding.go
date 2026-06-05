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

// SkipMaskBinIn is the input structure for serializing a skip mask.
// It contains a slice of Local IDs (LIDs) that correspond to documents
// matching the skip mask query criteria.
type SkipMaskBinIn struct {
	LIDs []seq.LID
}

// SkipMaskBinOut is the output structure for deserialized skip mask data.
// After unmarshaling, LIDs are converted to uint32 array.
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

// lidsCodec represents the compression codec used for LIDs block encoding.
type lidsCodec byte

const (
	lidsCodecDelta     = 1 // Delta-encoded varints without compression
	lidsCodecDeltaZstd = 2 // Delta-encoded varints with zstd compression
)

// lidsBlockHeader contains metadata for a block of LIDs.
// Each block stores a subset of LIDs (up to maxLIDsBlockLen) along with
// information needed to decode and locate the block data.
type lidsBlockHeader struct {
	Codec  lidsCodec // Compression codec used for this block (delta or delta+zstd)
	Length uint32    // Number of LIDs in this block
	MinLID uint32    // Minimum LID value in the block
	MaxLID uint32    // Maximum LID value in the block
	Size   uint32    // Size of the compressed block data in bytes
	Offset uint64    // Offset of the block data in the file
}

// marshal serializes the block header into the provided byte slice.
// The format is: Codec (1 byte) + Length (4 bytes) + MinLID (4 bytes) + MaxLID (4 bytes) + Size (4 bytes) + Offset (8 bytes) = 25 bytes.
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

// unmarshal deserializes a block header from the provided byte slice.
// Returns the remaining unconsumed bytes and any error encountered.
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

// marshalSkipMask serializes a skip mask into binary format.
// Returns the serialized data with the version byte prepended.
func marshalSkipMask(dst []byte, in *SkipMaskBinIn) []byte {
	dst = append(dst, uint8(skipMaskBinVersion1))
	dst = marshalLIDsBlocks(dst, in.LIDs)
	return dst
}

const (
	sizeOfUint32 = unsafe.Sizeof(uint32(0)) // 4 bytes
	sizeOfUint64 = unsafe.Sizeof(uint64(0)) // 8 bytes
)

const (
	// lidsBlockHeaderSizeBytes is the size of a single block header in bytes: 1 (Codec) + 4*4 (Length, MinLID, MaxLID, Size) + 8 (Offset) = 25
	lidsBlockHeaderSizeBytes = 1 + (4 * sizeOfUint32) + sizeOfUint64
	// maxLIDsBlockLen is the maximum number of LIDs stored in a single block
	maxLIDsBlockLen = 1024
)

var lidsBlockBufPool util.BufferPool

// marshalLIDsBlocks splits the input LIDs into blocks and serializes them.
// Each block contains up to maxLIDsBlockLen LIDs. The output format is:
// [number of blocks: 4 bytes] [block 1 header] [block 2 header] ... [block 1 data] [block 2 data] ...
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

// marshalLIDsBlock encodes a slice of LIDs using delta compression.
// It first computes delta-encoded varints, then attempts zstd compression.
// If zstd provides at least 5% compression, it uses zstd; otherwise, it stores
// the raw delta-encoded data. Returns the encoded data and the codec used.
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

// unmarshalSkipMask deserializes a skip mask from binary format.
// Validates the version and delegates to unmarshalLIDsBlocks for block processing.
func unmarshalSkipMask(dst *SkipMaskBinOut, src []byte) (_ []byte, err error) {
	if len(src) < minSkipMaskBytesLen {
		return nil, fmt.Errorf("invalid skip mask format; want %d bytes, got %d", minSkipMaskBytesLen, len(src))
	}

	version := skipMaskBinVersion(src[0])
	src = src[1:]
	if _, ok := availableVersions[version]; !ok {
		return nil, fmt.Errorf("invalid skip mask binary version: %d", version)
	}

	src, err = unmarshalLIDsBlocks(src, func(lid uint32) {
		dst.LIDs = append(dst.LIDs, lid)
	})
	if err != nil {
		return src, err
	}

	return src, nil
}

// unmarshalLIDsBlocks reads all LIDs blocks from the source data.
// First reads the number of blocks, then parses each block header,
// and finally decodes each block's data.
func unmarshalLIDsBlocks(src []byte, add func(uint32)) ([]byte, error) {
	numberOfBlocks := binary.LittleEndian.Uint32(src)
	src = src[sizeOfUint32:]

	var err error

	headers := make([]lidsBlockHeader, 0, numberOfBlocks)
	for range numberOfBlocks {
		header := lidsBlockHeader{}
		src, err = header.unmarshal(src)
		if err != nil {
			return src, fmt.Errorf("can't unmarshal lids header: %s", err)
		}
		headers = append(headers, header)
	}

	for i := range numberOfBlocks {
		src, err = unmarshalLIDsBlock(src, headers[i], add)
		if err != nil {
			return src, err
		}
	}

	if len(src) > 0 {
		return src, fmt.Errorf("unexpected tail when unmarshaling LIDs blocks")
	}

	return src, nil
}

// unmarshalLIDsBlock decodes a single LIDs block based on its header.
// Handles both compressed (zstd) and uncompressed codec types.
func unmarshalLIDsBlock(src []byte, header lidsBlockHeader, add func(uint32)) ([]byte, error) {
	if len(src) == 0 {
		return src, fmt.Errorf("empty LIDs block")
	}

	if header.Size == 0 || int(header.Size) > len(src) {
		return src, fmt.Errorf("invalid LIDs block length %d; want %d", len(src), header.Size)
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
			return src, fmt.Errorf("can't decompress ids block: %s", err)
		}
		err = unmarshalLIDsDelta(b.B, header, add)
		if err != nil {
			return src, err
		}
		return src, nil
	case lidsCodecDelta:
		err = unmarshalLIDsDelta(block, header, add)
		if err != nil {
			return src, err
		}
		return src, nil
	default:
		return src, fmt.Errorf("unknown ids codec: %d", header.Codec)
	}
}

func unmarshalLIDsDelta(block []byte, header lidsBlockHeader, add func(uint32)) error {
	prevLID := uint32(0)
	for range header.Length {
		v, n := binary.Varint(block)
		block = block[n:]
		lid := prevLID + uint32(v)
		prevLID = lid
		add(lid)
	}

	if len(block) > 0 {
		return fmt.Errorf("unexpected tail when unmarshaling LIDs block")
	}

	return nil
}

// getCompressLevel returns the appropriate zstd compression level based on data size.
// Higher compression levels are used for larger data to achieve better ratios.
// Returns: 1 for <=512 bytes, 2 for <=4KB, 3 for larger data.
func getCompressLevel(size int) int {
	level := 3
	if size <= 512 {
		level = 1
	} else if size <= 4*1024 {
		level = 2
	}
	return level
}
