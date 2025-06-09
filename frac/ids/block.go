package ids

import (
	"encoding/binary"

	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/seq"
)

type Block struct {
	MIDs []uint64
	RIDs []uint64
	Pos  []uint64
}

func (b *Block) GetMinID() seq.ID {
	return seq.ID{
		MID: b.minMID(),
		RID: b.minRID(),
	}
}

func (b *Block) minMID() seq.MID {
	return seq.MID(b.MIDs[len(b.MIDs)-1])
}

func (b *Block) minRID() seq.RID {
	return seq.RID(b.RIDs[len(b.RIDs)-1])
}

func (b *Block) GetExtForRegistry() (uint64, uint64) {
	return uint64(b.minMID()), uint64(b.minRID())
}

func (b *Block) PackMIDs(p *packer.BytesPacker) {
	p.Data = packRawIDsVarint(b.MIDs, p.Data)
}

func (b *Block) PackRIDs(p *packer.BytesPacker) {
	p.Data = packRawIDsNoVarint(b.RIDs, p.Data)
}

func (b *Block) PackPos(p *packer.BytesPacker) {
	p.Data = packRawIDsVarint(b.Pos, p.Data)
}

func packRawIDsVarint(src []uint64, dst []byte) []byte {
	var prev uint64
	for _, val := range src {
		dst = binary.AppendUvarint(dst, val-prev)
		prev = val
	}
	return dst
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

func packRawIDsNoVarint(src []uint64, dst []byte) []byte {
	for _, val := range src {
		dst = binary.LittleEndian.AppendUint64(dst, val)
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
