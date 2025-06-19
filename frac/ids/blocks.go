package ids

import (
	"encoding/binary"

	"github.com/ozontech/seq-db/conf"
)

type BlockMIDs struct {
	Values []uint64
}

func (b *BlockMIDs) Pack(buf []byte) []byte {
	var prev uint64
	for _, mid := range b.Values {
		buf = binary.AppendVarint(buf, int64(mid-prev))
		prev = mid
	}
	return buf
}

func (b *BlockMIDs) Unpack(data []byte) {
	b.Values = unpackRawIDsVarint(data, b.Values[:0])
}

type BlockRIDs struct {
	version conf.BinaryDataVersion
	Values  []uint64
}

func (b *BlockRIDs) Pack(buf []byte) []byte {
	for _, rid := range b.Values {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(rid))
	}
	return buf
}

func (b *BlockRIDs) Unpack(data []byte) {
	if b.version < conf.BinaryDataV1 {
		b.Values = unpackRawIDsVarint(data, b.Values[:0])
		return
	}
	b.Values = unpackRawIDsNoVarint(data, b.Values[:0])
}

type BlockParams struct {
	Values []uint64
}

func (b *BlockParams) Pack(buf []byte) []byte {
	var prev uint64
	for _, pos := range b.Values {
		buf = binary.AppendVarint(buf, int64(pos-prev))
		prev = pos
	}
	return buf
}

func (b *BlockParams) Unpack(data []byte) {
	b.Values = unpackRawIDsVarint(data, b.Values[:0])
}

func unpackRawIDsVarint(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	id := uint64(0)
	for len(src) != 0 {
		delta, n := binary.Varint(src)
		if n <= 0 {
			panic("varint decoded with error")
		}
		src = src[n:]
		id += uint64(delta)
		dst = append(dst, id)
	}
	return dst
}

func unpackRawIDsNoVarint(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint64(src))
		src = src[8:]
	}
	return dst
}
