package lids

import (
	"encoding/binary"
	"unsafe"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/packer"
)

type Block struct {
	LIDs    []uint32
	Offsets []uint32
	// todo remove this legacy field
	IsLastLID bool
}

func (b *Block) getCount() int {
	return len(b.Offsets) - 1
}

func (b *Block) getLIDs(i int) []uint32 {
	return b.LIDs[b.Offsets[i]:b.Offsets[i+1]]
}

func (b *Block) Pack(dst []byte) []byte {
	// TODO store next flags into a single byte
	// write b.IsLastLID as a dedicated uint32 in the header of block
	switch b.IsLastLID {
	case true:
		dst = binary.LittleEndian.AppendUint32(dst, 1)
	case false:
		dst = binary.LittleEndian.AppendUint32(dst, 0)
	}

	fullBlock := len(b.LIDs) == consts.LIDBlockCap
	switch fullBlock {
	case true:
		dst = binary.LittleEndian.AppendUint32(dst, 1)
	case false:
		dst = binary.LittleEndian.AppendUint32(dst, 0)
	}

	if len(b.LIDs) == consts.LIDBlockCap {
		offsetPacker := packer.NewBitpacker(dst, 128)
		offsetPacker.Append(b.Offsets)
		dst = offsetPacker.Close()
		lidPacker := packer.NewBitpacker(dst, 128)
		dst = lidPacker.Append4kBlock(b.LIDs)
	} else {
		lidPacker := packer.NewBitpacker(dst, 128)
		sep := []uint32{0}
		last := b.getCount() - 1
		for i := 0; i <= last; i++ {
			lidPacker.Append(b.getLIDs(i))
			lidPacker.Append(sep)
		}
		dst = lidPacker.Close()
	}
	return dst
}

func (b *Block) GetSizeBytes() int {
	const (
		uint32Size = int(unsafe.Sizeof(uint32(0)))
		blockSize  = int(unsafe.Sizeof(*b))
	)
	return blockSize + uint32Size*cap(b.LIDs) + uint32Size*cap(b.Offsets)
}

// TODO add support of the previous versions
func (b *Block) Unpack(data []byte, buf *UnpackBuffer) error {
	unpacker := packer.NewBytesUnpacker(data)
	buf.Reset()

	// read IsLastLID from a dedicated uint32
	isLastLIDValue := unpacker.GetUint32()
	switch isLastLIDValue {
	case 1:
		b.IsLastLID = true
	case 0:
		b.IsLastLID = false
	}

	fullBlock := unpacker.GetUint32()
	switch fullBlock {
	case 1:
		// block has exactly consts.LIDBlockCap LIDs
		decompressedChunk := buf.decompressed
		compressedChunk := buf.compressed
		offsetUnpacker := packer.NewBitpackUnpacker(unpacker, decompressedChunk, compressedChunk)
		for {
			offsetChunk, ok := offsetUnpacker.NextChunk()
			if !ok {
				break
			}
			b.Offsets = append(b.Offsets, offsetChunk...)
		}

		lidUnpacker := packer.NewBitpackUnpacker(unpacker, decompressedChunk, compressedChunk)
		b.LIDs = lidUnpacker.AllocateAndRead4kChunk()
	case 0:
		decompressedChunk := buf.decompressed
		compressedChunk := buf.compressed
		buf.offsets = append(buf.offsets, 0)

		bitpackUnpacker := packer.NewBitpackUnpacker(unpacker, decompressedChunk, compressedChunk)
		pos := 0
		for {
			chunk, ok := bitpackUnpacker.NextChunk()
			if !ok {
				break
			}

			for _, lid := range chunk {
				if pos > 0 && lid == 0 {
					b.LIDs = append(b.LIDs, buf.lids...)
					buf.lids = buf.lids[:0]
					buf.offsets = append(buf.offsets, uint32(pos))
				} else {
					buf.lids = append(buf.lids, lid)
					pos++
				}
			}
		}

		b.Offsets = append([]uint32{}, buf.offsets...)
	}
	return nil
}
