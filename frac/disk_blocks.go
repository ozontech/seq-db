package frac

import (
	"encoding/binary"
	"math"

	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/seq"
)

type lidsBlockMeta struct {
	MinTID      uint32
	MaxTID      uint32
	IsContinued bool
}

func (m lidsBlockMeta) getExtForRegistry() (uint64, uint64) {
	var ext1, ext2 uint64
	if m.IsContinued {
		ext1 = 1
	}
	ext2 = uint64(m.MaxTID)<<32 | uint64(m.MinTID)
	return ext1, ext2
}

type DiskIDsBlock struct {
	ids []seq.ID
	pos []uint64
}

func (b *DiskIDsBlock) getMinID() seq.ID {
	return b.ids[len(b.ids)-1]
}

func (b *DiskIDsBlock) getExtForRegistry() (uint64, uint64) {
	last := b.getMinID()
	return uint64(last.MID), uint64(last.RID)
}

func (b *DiskIDsBlock) packMIDs(buf []byte) []byte {
	var mid, prev uint64
	for _, id := range b.ids {
		mid = uint64(id.MID)
		buf = binary.AppendVarint(buf, int64(mid-prev))
		prev = mid
	}
	return buf
}

func (b *DiskIDsBlock) packRIDs(buf []byte) []byte {
	for _, id := range b.ids {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(id.RID))
	}
	return buf
}

func (b *DiskIDsBlock) packPos(buf []byte) []byte {
	var prev uint64
	for _, pos := range b.pos {
		buf = binary.AppendVarint(buf, int64(pos-prev))
		prev = pos
	}
	return buf
}

type DiskTokensBlock struct {
	field            string
	isStartOfField   bool
	totalSizeOfField int
	startTID         uint32
	tokens           [][]byte
}

func (t *DiskTokensBlock) createTokenTableEntry(startIndex, blockIndex uint32) *token.TableEntry {
	size := len(t.tokens)
	return &token.TableEntry{
		StartIndex: startIndex,
		StartTID:   t.startTID,
		ValCount:   uint32(size),
		BlockIndex: blockIndex,
		MaxVal:     string(t.tokens[size-1]),
	}
}

func (t *DiskTokensBlock) pack(buf []byte) []byte {
	for _, token := range t.tokens {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(token)))
		buf = append(buf, token...)
	}
	buf = binary.LittleEndian.AppendUint32(buf, math.MaxUint32)
	return buf
}
