package packer

import (
	"unsafe"

	"github.com/ronanh/intcomp"
)

// TODO the whole file looks pretty unreadable

// TODO use golang iter?
// ChunkIterator allows to iterate of chunks of fixed number (usually 128). The last chunk might be less
// than 128 numbers
type ChunkIterator interface {
	NextChunk() ([]uint32, bool)
}

type BitpackUnpacker struct {
	unpacker *BytesUnpacker

	// TODO bad design: a caller must provided dst buffer on Next() call, we shall not own it
	decompressedBuf []uint32 // descompressed buf, used to return in iterator
	compressedBuf   []uint32 // temporary buf, intcomp works on top of []uint32 slices
	done            bool
}

var _ ChunkIterator = (*BitpackUnpacker)(nil)

func isLittleEndian() bool {
	// TODO check binary.NativeEndian == littleEndian?
	return true
}

func NewBitpackUnpacker(unpacker *BytesUnpacker, decompressedBuf []uint32, compressedBuf []uint32) *BitpackUnpacker {
	return &BitpackUnpacker{
		unpacker:        unpacker,
		decompressedBuf: decompressedBuf,
		compressedBuf:   compressedBuf,
	}
}

// SkipChunks allows to skip chunks to navigate directly to chunk needed
func (b *BitpackUnpacker) SkipChunks(chunks int) bool {
	for i := 0; i < chunks; i++ {
		if !b.SkipChunk() {
			return false
		}
	}
	return true
}

func (b *BitpackUnpacker) SkipChunk() bool {
	if b.done || b.unpacker.Len() == 0 {
		return false
	}

	blockLen, err := b.unpacker.GetVarint()
	if err != nil {
		return false
	}

	if blockLen == 0 {
		b.readVarintChunk()
		// we return false, since varint chunk is always the last, so there is no chunks left
		return false
	}

	// -1 indicates end of all data
	if blockLen < 0 {
		return false
	}
	b.unpacker.SkipUints32(int(blockLen))
	return true
}

// TODO use directly intcomp for that
func (b *BitpackUnpacker) AllocateAndRead4kChunk() []uint32 {
	blockLen, err := b.unpacker.GetVarint()
	if err != nil {
		return nil
	}

	b.compressedBuf = b.compressedBuf[:0]
	b.readUint32Block(blockLen)

	_, decompressed := intcomp.UncompressDeltaBinPackUint32(b.compressedBuf, nil)
	return decompressed
}

// NextChunk returns the next decompressed chunk of values.
// Returns nil, false when all chunks have been read.
func (b *BitpackUnpacker) NextChunk() ([]uint32, bool) {
	if b.done || b.unpacker.Len() == 0 {
		return nil, false
	}

	// read the start of the chunk.
	// TODO it's a space overhead
	blockLen, err := b.unpacker.GetVarint()
	if err != nil {
		return nil, false
	}

	if blockLen == 0 {
		return b.readVarintChunk()
	}
	if blockLen < 0 {
		return nil, false
	}

	b.compressedBuf = b.compressedBuf[:0]
	b.readUint32Block(blockLen)

	b.decompressedBuf = b.decompressedBuf[:0]
	_, b.decompressedBuf = intcomp.UncompressDeltaBinPackUint32(b.compressedBuf, b.decompressedBuf)
	return b.decompressedBuf, true
}

// readVarintChunk reads the varint chunk (there is at most one at the end)
func (b *BitpackUnpacker) readVarintChunk() ([]uint32, bool) {
	if b.done {
		return nil, false
	}

	varintCount, err := b.unpacker.GetVarint()
	if err != nil {
		return nil, false
	}

	if varintCount == 0 {
		sentinel, err := b.unpacker.GetVarint()
		if err != nil || sentinel != -1 {
			return nil, false
		}
		b.done = true
		return nil, false
	}

	chunk := b.decompressedBuf[0:int(varintCount)]
	for i := int64(0); i < varintCount; i++ {
		val, err := b.unpacker.GetVarint()
		if err != nil {
			return nil, false
		}
		chunk[i] = uint32(val)
	}

	sentinel, err := b.unpacker.GetVarint()
	if err != nil || sentinel != -1 {
		return nil, false
	}

	b.done = true
	return chunk, true
}

// readUint32Block reads the next blockLen bytes of data into compressedBuf - a []uint32 buffer
func (b *BitpackUnpacker) readUint32Block(blockLen int64) {
	if isLittleEndian() {
		// TODO use apache arrow or some other lib to avoid this shuit?
		if cap(b.compressedBuf) < int(blockLen) {
			b.compressedBuf = make([]uint32, blockLen)
		} else {
			b.compressedBuf = b.compressedBuf[:blockLen]
		}

		byteCount := int(blockLen) * 4
		buf := b.unpacker.GetBuffer()
		if len(buf) < byteCount {
			b.compressedBuf = b.compressedBuf[:0]
			for i := int64(0); i < blockLen; i++ {
				b.compressedBuf = append(b.compressedBuf, b.unpacker.GetUint32())
			}
		} else {
			src := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(buf[:byteCount]))), blockLen)
			copy(b.compressedBuf, src)
			b.unpacker.SkipUints32(int(blockLen))
		}
	} else {
		// slow path, unpack with binary.LittleEndian.Uint32
		for i := int64(0); i < blockLen; i++ {
			b.compressedBuf = append(b.compressedBuf, b.unpacker.GetUint32())
		}
	}
}
