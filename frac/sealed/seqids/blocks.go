package seqids

import (
	"encoding/binary"

	"github.com/ozontech/seq-db/seq"
)

type DiskIDsBlock struct {
	IDs []seq.ID
	Pos []uint64
}

func (b *DiskIDsBlock) GetMinID() seq.ID {
	return b.IDs[len(b.IDs)-1]
}

func (b *DiskIDsBlock) GetExtForRegistry() (uint64, uint64) {
	last := b.GetMinID()
	return uint64(last.MID), uint64(last.RID)
}

func (b *DiskIDsBlock) PackMIDs(dst []byte) []byte {
	var mid, prev uint64
	for _, id := range b.IDs {
		mid = uint64(id.MID)
		dst = binary.AppendVarint(dst, int64(mid-prev))
		prev = mid
	}
	return dst
}

func (b *DiskIDsBlock) PackRIDs(dst []byte) []byte {
	for _, id := range b.IDs {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(id.RID))
	}
	return dst
}

func (b *DiskIDsBlock) PackPos(dst []byte) []byte {
	var prev uint64
	for _, pos := range b.Pos {
		dst = binary.AppendVarint(dst, int64(pos-prev))
		prev = pos
	}
	return dst
}
