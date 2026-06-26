package sealed

import (
	"encoding/binary"
	"errors"
)

type BlockOffsets struct {
	Offsets []uint64
}

func (b *BlockOffsets) Pack(buf []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.Offsets)))

	// NOTE(dkharms): Previously we stored here amount of documents ids.
	//
	// I've created a task which will require fraction binary version bumping
	// to get rid of this: https://github.com/ozontech/seq-db/issues/409
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	var prev uint64
	for _, pos := range b.Offsets {
		buf = binary.AppendVarint(buf, int64(pos-prev))
		prev = pos
	}
	return buf
}

func (b *BlockOffsets) Unpack(data []byte) error {
	if len(data) < 4 {
		return errors.New("blocks offset decoding error: truncated header (missing offsets count)")
	}

	idsBlocksCount := binary.LittleEndian.Uint32(data)
	data = data[4:]

	if len(data) < 4 {
		return errors.New("blocks offset decoding error: truncated header (missing IDsTotal)")
	}

	// NOTE(dkharms): Previously we stored here amount of documents ids.
	_ = binary.LittleEndian.Uint32(data)
	data = data[4:]

	offset := uint64(0)
	b.Offsets = make([]uint64, 0, idsBlocksCount)
	for len(data) != 0 {
		delta, n := binary.Varint(data)
		if n == 0 {
			return errors.New("blocks offset decoding error: varint returned 0")
		}

		if n < 0 {
			return errors.New("blocks offset decoding error: varint overflow")
		}

		data = data[n:]
		offset += uint64(delta)

		b.Offsets = append(b.Offsets, offset)
	}

	if uint32(len(b.Offsets)) != idsBlocksCount {
		return errors.New("blocks offset decoding error: offset count mismatch")
	}

	return nil
}
