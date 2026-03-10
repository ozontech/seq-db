package packer

import (
	"encoding/binary"
	"unsafe"

	"github.com/ronanh/intcomp"
)

// TODO the whole file looks pretty unreadable

type Bitpacker struct {
	dst []byte // output buffer
	// TODO need to remove this chunk, use slice from input slice instead
	chunk           []uint32 // temporary chunk to feed to intcomp library
	compressedChunk []uint32 // temporary chunk to which intcomp will write compressed data
	chunkSize       int
}

func NewBitpacker(dst []byte, chunkSize int) *Bitpacker {
	if chunkSize <= 0 {
		chunkSize = 1024 // default chunk size
	}
	return &Bitpacker{
		dst:             dst,
		chunk:           make([]uint32, 0, chunkSize),
		compressedChunk: make([]uint32, 0, chunkSize/4),
		chunkSize:       chunkSize,
	}
}

// Append adds values to the current chunk. When the chunk is full, it compresses
// and writes it to dst.
func (b *Bitpacker) Append(values []uint32) {
	// TODO this is slow af
	// TODO copy values to chunk fully if values slice is large
	for _, val := range values {
		b.chunk = append(b.chunk, val)

		if len(b.chunk) == b.chunkSize {
			b.compressChunk()
		}
	}
}

// TODO use intcomp directly for that
func (b *Bitpacker) Append4kBlock(values []uint32) []byte {
	_, compressed := intcomp.CompressDeltaBinPackUint32(values, nil)
	b.dst = binary.AppendVarint(b.dst, int64(len(compressed)))

	// TODO memcpy (arrow?)
	for _, val := range compressed {
		b.dst = binary.LittleEndian.AppendUint32(b.dst, val)
	}
	return b.dst
}

func (b *Bitpacker) compressChunk() {
	if len(b.chunk) == 0 {
		return
	}

	_, b.compressedChunk = intcomp.CompressDeltaBinPackUint32(b.chunk, b.compressedChunk)
	b.dst = binary.AppendVarint(b.dst, int64(len(b.compressedChunk)))

	for _, val := range b.compressedChunk {
		b.dst = binary.LittleEndian.AppendUint32(b.dst, val)
	}

	b.chunk = b.chunk[:0]
	b.compressedChunk = b.compressedChunk[:0]
}

// Close writes a residual (less than 128 number) as varints
func (b *Bitpacker) Close() []byte {
	// append 0 - an indicator of the last chunk with varints
	b.dst = binary.AppendVarint(b.dst, 0)

	// append number of varints first
	b.dst = binary.AppendVarint(b.dst, int64(len(b.chunk)))

	for _, lid := range b.chunk {
		b.dst = binary.AppendVarint(b.dst, int64(lid))
	}

	// TODO remove?
	// append the trailer
	b.dst = binary.AppendVarint(b.dst, -1)
	return b.dst
}

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
