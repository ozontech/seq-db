package storage

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	MetaBlockMagic byte = 101

	offsetMetaBlockMagic           = 0  // 1 byte  (M) Magic byte (always 101)
	offsetMetaBlockVersion         = 1  // 1 byte  (V) Version
	offsetMetaBlockCodec           = 2  // 1 byte  (C) Codec
	offsetMetaBlockLength          = 3  // 4 bytes (L) Length of payload
	offsetMetaBlockRawLength       = 7  // 4 bytes (U) Raw length (after decompression)
	offsetMetaBlockPayloadChecksum = 11 // 4 bytes (P) Payload checksum - covers payload only
	offsetMetaBlockDocsOffset      = 15 // 8 bytes (D) Docs offset
	offsetMetaBlockHeaderChecksum  = 23 // 4 bytes (H) Header checksum - covers bytes 0-22

	MetaBlockHeaderLen      = 27
	MetaBlockCurrentVersion = uint8(1)
)

// MetaBlock format: M : V : C : LLLL : UUUU : PPPP : DDDD-DDDD : HHHH
// M = Magic (101), V = Version, C = Codec, L = Length, U = Raw Length, P = Payload Checksum, D = Docs Offset, H = Header Checksum

type MetaBlock []byte

func (b MetaBlock) Magic() byte {
	return b[offsetMetaBlockMagic]
}

func (b MetaBlock) Version() uint8 {
	return b[offsetMetaBlockVersion]
}

func (b MetaBlock) SetVersion(version uint8) {
	b[offsetMetaBlockVersion] = version
}

func (b MetaBlock) Codec() Codec {
	return Codec(b[offsetMetaBlockCodec])
}

func (b MetaBlock) SetCodec(codecVal Codec) {
	b[offsetMetaBlockCodec] = byte(codecVal)
}

func (b MetaBlock) Len() uint32 {
	return binary.LittleEndian.Uint32(b[offsetMetaBlockLength:])
}

func (b MetaBlock) SetLen(val uint32) {
	binary.LittleEndian.PutUint32(b[offsetMetaBlockLength:], val)
}

func (b MetaBlock) FullLen() uint32 {
	return b.Len() + MetaBlockHeaderLen
}

func (b MetaBlock) CalcLen() {
	b.SetLen(uint32(len(b) - MetaBlockHeaderLen))
}

func (b MetaBlock) RawLen() uint32 {
	return binary.LittleEndian.Uint32(b[offsetMetaBlockRawLength:])
}

func (b MetaBlock) SetRawLen(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetMetaBlockRawLength:], x)
}

func (b MetaBlock) PayloadChecksum() uint32 {
	return binary.LittleEndian.Uint32(b[offsetMetaBlockPayloadChecksum:])
}

func (b MetaBlock) SetPayloadChecksum(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetMetaBlockPayloadChecksum:], x)
}

func (b MetaBlock) CalcPayloadChecksum() {
	b.SetPayloadChecksum(crc32.ChecksumIEEE(b.Payload()))
}

func (b MetaBlock) HeaderChecksum() uint32 {
	return binary.LittleEndian.Uint32(b[offsetMetaBlockHeaderChecksum:])
}

func (b MetaBlock) SetHeaderChecksum(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetMetaBlockHeaderChecksum:], x)
}

func (b MetaBlock) CalcHeaderChecksum() {
	b.SetHeaderChecksum(crc32.ChecksumIEEE(b[:offsetMetaBlockHeaderChecksum]))
}

func (b MetaBlock) DocsOffset() uint64 {
	return binary.LittleEndian.Uint64(b[offsetMetaBlockDocsOffset:])
}

// SetDocsOffset updates docs offset. It will also recalc header checksum (cheap).
func (b MetaBlock) SetDocsOffset(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetMetaBlockDocsOffset:], x)
	b.CalcHeaderChecksum()
}

func (b MetaBlock) Payload() []byte {
	return b[MetaBlockHeaderLen:]
}

// IsCorrect checks if this is a correct meta block by checking header and payload checksums
func (b MetaBlock) IsCorrect() bool {
	return b.IsHeaderCorrect() && b.IsPayloadCorrect()
}

// IsHeaderCorrect checks if header checksum is correct
func (b MetaBlock) IsHeaderCorrect() bool {
	return crc32.ChecksumIEEE(b[:offsetMetaBlockHeaderChecksum]) == b.HeaderChecksum()
}

// IsPayloadCorrect checks if payload checksum is valid
func (b MetaBlock) IsPayloadCorrect() bool {
	return crc32.ChecksumIEEE(b.Payload()) == b.PayloadChecksum()
}

// IsMetaBlock checks if this data is possibly a meta block.
// Returns true if the data has at least MetaBlockHeaderLen bytes and starts with magic byte.
// This doesn't check for corruption, use IsCorrect() for checksum validation.
func IsMetaBlock(data []byte) bool {
	return len(data) >= MetaBlockHeaderLen && data[0] == MetaBlockMagic
}

func CompressMetaBlock(src []byte, dst MetaBlock, zstdLevel int) MetaBlock {
	dst = append(dst[:0], make([]byte, MetaBlockHeaderLen)...) // fill header with zeros for cleanup
	dst = zstd.CompressLevel(src, dst, zstdLevel)

	dst[offsetMetaBlockMagic] = MetaBlockMagic
	dst.SetVersion(MetaBlockCurrentVersion)
	dst.CalcLen()
	dst.SetRawLen(uint32(len(src)))
	dst.SetCodec(CodecZSTD)
	dst.CalcPayloadChecksum()
	dst.CalcHeaderChecksum()

	return dst
}

func PackMetaBlock(payload []byte, dst MetaBlock) MetaBlock {
	dst = append(dst[:0], make([]byte, MetaBlockHeaderLen)...) // fill header with zeros for cleanup
	dst = append(dst, payload...)

	dst[offsetMetaBlockMagic] = MetaBlockMagic
	dst.SetVersion(MetaBlockCurrentVersion)
	dst.CalcLen()
	dst.SetRawLen(uint32(len(payload)))
	dst.SetCodec(CodecNo)
	dst.CalcPayloadChecksum()
	dst.CalcHeaderChecksum()

	return dst
}

// PackMetaBlockToDocBlock converts MetaBlock to legacy DocBlock.
func PackMetaBlockToDocBlock(metaBlock MetaBlock, dst DocBlock) DocBlock {
	dst = append(dst[:0], make([]byte, DocBlockHeaderLen)...)
	dst = append(dst, metaBlock.Payload()...)

	dst.CalcLen()
	dst.SetRawLen(uint64(metaBlock.RawLen()))
	dst.SetCodec(metaBlock.Codec())
	dst.SetExt2(metaBlock.DocsOffset())

	return dst
}

// PackDocBlockToMetaBlock converts DocBlock to MetaBlock in place without copying payload.
// docBlock will be invalid after packing
func PackDocBlockToMetaBlock(docBlock DocBlock) MetaBlock {
	rawLen := uint32(docBlock.RawLen())
	codec := docBlock.Codec()
	docsOffset := docBlock.GetExt2()
	payloadLen := uint32(len(docBlock) - DocBlockHeaderLen)

	const headerDiff = DocBlockHeaderLen - MetaBlockHeaderLen
	mb := MetaBlock(docBlock[headerDiff:])

	mb[offsetMetaBlockMagic] = MetaBlockMagic
	mb.SetVersion(MetaBlockCurrentVersion)
	mb.SetLen(payloadLen)
	mb.SetRawLen(rawLen)
	mb.SetCodec(codec)
	// write docs offset directly since SetDocsOffset recalculates header checksum
	binary.LittleEndian.PutUint64(mb[offsetMetaBlockDocsOffset:], docsOffset)
	mb.CalcPayloadChecksum()
	mb.CalcHeaderChecksum()

	return mb
}

// DecompressTo always put the result in `dst` regardless of whether unpacking is required
// or part of the MetaBlock can be enough.
//
// So MetaBlock does not share the same data with `dst` and can be used safely
func (b MetaBlock) DecompressTo(dst []byte) ([]byte, error) {
	payload := b.Payload()
	if b.Codec() == CodecNo {
		dst = util.EnsureSliceSize(dst, int(b.RawLen()))
		copy(dst, payload)
		return dst, nil
	}
	return b.Codec().decompressBlock(int(b.RawLen()), payload, dst)
}
