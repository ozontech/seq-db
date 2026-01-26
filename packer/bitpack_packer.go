package packer

import (
	"encoding/binary"

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
