package disk

import "encoding/binary"

const (
	offsetBlockCodec  = 0  // 1 byte  (C) Codec
	offsetBlockLen    = 1  // 4 bytes (L) Length
	offsetBlockRawLen = 5  // 4 bytes (R) Raw Length
	offsetBlockExt1   = 9  // 8 bytes (E) Extensions/flags
	offsetBlockExt2   = 17 // 8 bytes (E) Extensions/flags
	offsetBlockPos    = 25 // 8 bytes (P) Position

	BlocksRegistryEntrySize = 33
)

// BlocksRegistryEntry format: C : LLLL : RRRR : EEEE-EEEE-EEEE-EEEE : PPPP-PPPP
// See: /docs/format-index-file.md

type BlocksRegistryEntry []byte

func NewEmptyBlocksRegistryEntry() BlocksRegistryEntry {
	return make(BlocksRegistryEntry, BlocksRegistryEntrySize)
}

func NewBlocksRegistryEntry(pos int64, ext1, ext2 uint64, origBuff, finalBuf []byte, codec Codec) BlocksRegistryEntry {
	header := NewEmptyBlocksRegistryEntry()
	header.SetExt1(ext1)
	header.SetExt2(ext2)
	header.SetLen(uint32(len(finalBuf)))
	header.SetRawLen(uint32(len(origBuff)))
	header.SetCodec(codec)
	header.SetPos(uint64(pos))
	return header
}

func (b BlocksRegistryEntry) Codec() Codec {
	return Codec(b[offsetBlockCodec])
}

func (b BlocksRegistryEntry) SetCodec(codecVal Codec) {
	b[offsetBlockCodec] = byte(codecVal)
}

func (b BlocksRegistryEntry) Len() uint32 {
	return binary.LittleEndian.Uint32(b[offsetBlockLen:])
}

func (b BlocksRegistryEntry) SetLen(val uint32) {
	binary.LittleEndian.PutUint32(b[offsetBlockLen:], val)
}

func (b BlocksRegistryEntry) RawLen() uint32 {
	return binary.LittleEndian.Uint32(b[offsetBlockRawLen:])
}

func (b BlocksRegistryEntry) SetRawLen(x uint32) {
	binary.LittleEndian.PutUint32(b[offsetBlockRawLen:], x)
}

func (b BlocksRegistryEntry) GetExt1() uint64 {
	return binary.LittleEndian.Uint64(b[offsetBlockExt1:])
}

func (b BlocksRegistryEntry) SetExt1(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetBlockExt1:], x)
}

func (b BlocksRegistryEntry) GetExt2() uint64 {
	return binary.LittleEndian.Uint64(b[offsetBlockExt2:])
}

func (b BlocksRegistryEntry) SetExt2(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetBlockExt2:], x)
}

func (b BlocksRegistryEntry) GetPos() uint64 {
	return binary.LittleEndian.Uint64(b[offsetBlockPos:])
}

func (b BlocksRegistryEntry) SetPos(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetBlockPos:], x)
}
