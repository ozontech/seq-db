package storage

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	WalBlockMagic byte = 101

	offsetWalBlockMagic           = 0  // 1 byte  (M) Magic byte (always 101)
	offsetWalBlockVersion         = 1  // 1 byte  (V) Version
	offsetWalBlockCodec           = 2  // 1 byte  (C) Codec
	offsetWalBlockLength          = 3  // 4 bytes (L) Length of payload
	offsetWalBlockRawLength       = 7  // 4 bytes (U) Raw length (after decompression)
	offsetWalBlockPayloadChecksum = 11 // 4 bytes (P) Payload checksum - covers payload only
	offsetWalBlockDocsOffset      = 15 // 8 bytes (D) Docs offset
	offsetWalBlockHeaderChecksum  = 23 // 4 bytes (H) Header checksum - covers bytes 0-22

	WalBlockHeaderLen      = 27
	WalBlockCurrentVersion = uint8(1)
)

// WalBlock format: M : V : C : LLLL : UUUU : PPPP : DDDD-DDDD : HHHH
// M = Magic (101), V = Version, C = Codec, L = Length, U = Raw Length, P = Payload Checksum, D = Docs Offset, H = Header Checksum

type WalBlock []byte

func (b WalBlock) Magic() byte {
	return b[offsetWalBlockMagic]
}

func (b WalBlock) Version() uint8 {
	return b[offsetWalBlockVersion]
}

func (b WalBlock) SetVersion(version uint8) {
	b[offsetWalBlockVersion] = version
}

func (b WalBlock) Codec() Codec {
	return Codec(b[offsetWalBlockCodec])
}

func (b WalBlock) SetCodec(codecVal Codec) {
	b[offsetWalBlockCodec] = byte(codecVal)
}

func (b WalBlock) Len() uint32 {
	return binary.LittleEndian.Uint32(b[offsetWalBlockLength:])
}

func (b WalBlock) SetLen(val uint32) {
	binary.LittleEndian.PutUint32(b[offsetWalBlockLength:], val)
}

func (b WalBlock) FullLen() uint32 {
	return b.Len() + WalBlockHeaderLen
}

func (b WalBlock) CalcLen() {
	b.SetLen(uint32(len(b) - WalBlockHeaderLen))
}

func (b WalBlock) RawLen() uint32 {
	return binary.LittleEndian.Uint32(b[offsetWalBlockRawLength:])
}

func (b WalBlock) SetRawLen(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetWalBlockRawLength:], x)
}

func (b WalBlock) PayloadChecksum() uint32 {
	return binary.LittleEndian.Uint32(b[offsetWalBlockPayloadChecksum:])
}

func (b WalBlock) SetPayloadChecksum(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetWalBlockPayloadChecksum:], x)
}

func (b WalBlock) CalcPayloadChecksum() {
	b.SetPayloadChecksum(crc32.ChecksumIEEE(b.Payload()))
}

func (b WalBlock) HeaderChecksum() uint32 {
	return binary.LittleEndian.Uint32(b[offsetWalBlockHeaderChecksum:])
}

func (b WalBlock) SetHeaderChecksum(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetWalBlockHeaderChecksum:], x)
}

func (b WalBlock) CalcHeaderChecksum() {
	b.SetHeaderChecksum(crc32.ChecksumIEEE(b[:offsetWalBlockHeaderChecksum]))
}

func (b WalBlock) DocsOffset() uint64 {
	return binary.LittleEndian.Uint64(b[offsetWalBlockDocsOffset:])
}

// SetDocsOffset updates docs offset. It will also recalc header checksum (cheap).
func (b WalBlock) SetDocsOffset(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetWalBlockDocsOffset:], x)
	b.CalcHeaderChecksum()
}

func (b WalBlock) Payload() []byte {
	return b[WalBlockHeaderLen:]
}

// IsCorrect checks if this is a correct meta block by checking header and payload checksums
func (b WalBlock) IsCorrect() bool {
	return b.IsHeaderCorrect() && b.IsPayloadCorrect()
}

// IsHeaderCorrect checks if header checksum is correct
func (b WalBlock) IsHeaderCorrect() bool {
	return crc32.ChecksumIEEE(b[:offsetWalBlockHeaderChecksum]) == b.HeaderChecksum()
}

// IsPayloadCorrect checks if payload checksum is valid
func (b WalBlock) IsPayloadCorrect() bool {
	return crc32.ChecksumIEEE(b.Payload()) == b.PayloadChecksum()
}

// IsWalBlock checks if this data is possibly a meta block.
// Returns true if the data has at least WalBlockHeaderLen bytes and starts with magic byte.
// This doesn't check for corruption, use IsCorrect() for checksum validation.
func IsWalBlock(data []byte) bool {
	return len(data) >= WalBlockHeaderLen && data[0] == WalBlockMagic
}

func CompressWalBlock(src []byte, dst WalBlock, zstdLevel int) WalBlock {
	dst = append(dst[:0], make([]byte, WalBlockHeaderLen)...) // fill header with zeros for cleanup
	dst = zstd.CompressLevel(src, dst, zstdLevel)

	dst[offsetWalBlockMagic] = WalBlockMagic
	dst.SetVersion(WalBlockCurrentVersion)
	dst.CalcLen()
	dst.SetRawLen(uint32(len(src)))
	dst.SetCodec(CodecZSTD)
	dst.CalcPayloadChecksum()
	dst.CalcHeaderChecksum()

	return dst
}

func PackWalBlock(payload []byte, dst WalBlock) WalBlock {
	dst = append(dst[:0], make([]byte, WalBlockHeaderLen)...) // fill header with zeros for cleanup
	dst = append(dst, payload...)

	dst[offsetWalBlockMagic] = WalBlockMagic
	dst.SetVersion(WalBlockCurrentVersion)
	dst.CalcLen()
	dst.SetRawLen(uint32(len(payload)))
	dst.SetCodec(CodecNo)
	dst.CalcPayloadChecksum()
	dst.CalcHeaderChecksum()

	return dst
}

// DecompressTo always put the result in `dst` regardless of whether unpacking is required
// or part of the WalBlock can be enough.
//
// So WalBlock does not share the same data with `dst` and can be used safely
func (b WalBlock) DecompressTo(dst []byte) ([]byte, error) {
	payload := b.Payload()
	if b.Codec() == CodecNo {
		dst = util.EnsureSliceSize(dst, int(b.RawLen()))
		copy(dst, payload)
		return dst, nil
	}
	return b.Codec().decompressBlock(int(b.RawLen()), payload, dst)
}
