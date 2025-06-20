package lids

import (
	"math"
	"unsafe"

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

func (b *Block) Pack(p *packer.BytesPacker) {
	lastLID := int64(0)
	last := b.getCount() - 1

	for i := 0; i <= last; i++ {
		for _, lid := range b.getLIDs(i) {
			p.PutVarint(int64(lid) - lastLID)
			lastLID = int64(lid)
		}

		if i < last || b.IsLastLID {
			// when we add this value to prev we must get -1 (or math.MaxUint32 for uint32)
			// it is the end-marker; see `Block.Unpack()`
			p.PutVarint(-1 - lastLID)
		}
	}
}

func (b *Block) GetSizeBytes() int {
	const (
		uint32Size = int(unsafe.Sizeof(uint32(0)))
		blockSize  = int(unsafe.Sizeof(*b))
	)
	return blockSize + uint32Size*cap(b.LIDs) + uint32Size*cap(b.Offsets)
}

func (b *Block) Unpack(data []byte, buf *UnpackBuffer) error {
	var lid, offset uint32

	b.IsLastLID = true

	buf.lids = buf.lids[:0]
	buf.offsets = buf.offsets[:0]
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
