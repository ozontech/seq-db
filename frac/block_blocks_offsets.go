package frac

import (
	"encoding/binary"
	"errors"
)

type BlockBlocksOffsets struct {
	IDsTotal      uint32 // todo: the best place for this field is Info block
	BlocksOffsets []uint64
}

func (b *BlockBlocksOffsets) Pack(buf []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.BlocksOffsets)))
	buf = binary.LittleEndian.AppendUint32(buf, b.IDsTotal)

	var prev uint64
	for _, pos := range b.BlocksOffsets {
		buf = binary.AppendVarint(buf, int64(pos-prev))
		prev = pos
	}
	return buf
}

func (b *BlockBlocksOffsets) Unpack(data []byte) error {
	idsBlocksCount := binary.LittleEndian.Uint32(data)
	data = data[4:]

	b.IDsTotal = binary.LittleEndian.Uint32(data)
	data = data[4:]

	offset := uint64(0)
	b.BlocksOffsets = make([]uint64, 0, idsBlocksCount)
	for len(data) != 0 {
		delta, n := binary.Varint(data)
		if n == 0 {
			return errors.New("blocks offset decoding error: varint returned 0")
		}
		data = data[n:]
		offset += uint64(delta)
		b.BlocksOffsets = append(b.BlocksOffsets, offset)
	}
	return nil
}
