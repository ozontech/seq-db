package lids

import (
	"encoding/binary"
	"errors"
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/config"
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

func (b *Block) Pack(dst []byte, tmp []uint32) []byte {
	// TODO store next flags into a single byte
	// write b.IsLastLID as a dedicated uint32 in the header of block
	if b.IsLastLID {
		dst = binary.LittleEndian.AppendUint32(dst, 1)
	} else {
		dst = binary.LittleEndian.AppendUint32(dst, 0)
	}

	dst = packer.CompressDeltaBitpackUint32(dst, b.Offsets, tmp)
	dst = packer.CompressDeltaBitpackUint32(dst, b.LIDs, tmp)

	return dst
}

func (b *Block) GetSizeBytes() int {
	const (
		uint32Size = int(unsafe.Sizeof(uint32(0)))
		blockSize  = int(unsafe.Sizeof(*b))
	)
	return blockSize + uint32Size*cap(b.LIDs) + uint32Size*cap(b.Offsets)
}

func (b *Block) Unpack(data []byte, fracVer config.BinaryDataVersion, buf *UnpackBuffer) error {
	buf.Reset()

	if fracVer >= config.BinaryDataV3 {
		return b.unpackBitpack(data, buf)
	}

	return b.unpackVarint(data, buf)
}

func (b *Block) unpackBitpack(data []byte, buf *UnpackBuffer) error {
	// read IsLastLID from a dedicated uint32
	if len(data) < 4 {
		return errors.New("lids block decode error: truncated IsLastLID header")
	}
	isLastLIDValue := binary.LittleEndian.Uint32(data[:4])
	b.IsLastLID = isLastLIDValue == 1
	data = data[4:]

	var err error
	var values []uint32

	data, values, err = packer.DecompressDeltaBitpackUint32(data, buf.compressed, buf.decompressed)
	if err != nil {
		return err
	}
	b.Offsets = append([]uint32{}, values...)

	data, values, err = packer.DecompressDeltaBitpackUint32(data, buf.compressed, buf.decompressed)
	if err != nil {
		return err
	}
	b.LIDs = append([]uint32{}, values...)
	return nil
}

func (b *Block) unpackVarint(data []byte, buf *UnpackBuffer) error {
	var lid, offset uint32

	b.IsLastLID = true

	buf.offsets = append(buf.offsets, 0) // first offset is always zero

	unpacker := packer.NewBytesUnpacker(data)
	for unpacker.Len() > 0 {
		delta, err := unpacker.GetVarint()
		if err != nil {
			return err
		}
		lid += uint32(delta)

		if lid == math.MaxUint32 { // end of LIDs of current TID, see `Block.Pack()` method
			offset = uint32(len(buf.lids))
			buf.offsets = append(buf.offsets, offset)
			lid -= uint32(delta)
			continue
		}

		buf.lids = append(buf.lids, lid)
	}

	if int(offset) < len(buf.lids) {
		b.IsLastLID = false
		buf.offsets = append(buf.offsets, uint32(len(buf.lids)))
	}

	// copy from buffer
	b.LIDs = append([]uint32{}, buf.lids...)
	b.Offsets = append([]uint32{}, buf.offsets...)

	return nil
}
