package frac

import (
	"encoding/binary"
	"math"

	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/seq"
)

type lidsBlock struct {
	payload     lids.Block
	minTID      uint32
	maxTID      uint32
	isContinued bool
}

func (e lidsBlock) getExtForRegistry() (uint64, uint64) {
	var ext1, ext2 uint64
	if e.isContinued {
		ext1 = 1
	}
	ext2 = uint64(e.maxTID)<<32 | uint64(e.minTID)
	return ext1, ext2
}

type DiskInfoBlock struct {
	info *Info
}

func (b *DiskInfoBlock) pack(dst []byte) []byte {
	dst = append(dst, seqDBMagic...)
	dst = append(dst, b.info.Save()...)
	return dst
}

type DiskPositionsBlock struct {
	totalIDs uint32
	blocks   []uint64
}

func (b *DiskPositionsBlock) pack(dst []byte) []byte {
	var prev uint64
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.blocks)))
	dst = binary.LittleEndian.AppendUint32(dst, b.totalIDs)
	for _, pos := range b.blocks {
		dst = binary.AppendVarint(dst, int64(pos-prev))
		prev = pos
	}
	return dst
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

func (b *DiskIDsBlock) packMIDs(dst []byte) []byte {
	var mid, prev uint64
	for _, id := range b.ids {
		mid = uint64(id.MID)
		dst = binary.AppendVarint(dst, int64(mid-prev))
		prev = mid
	}
	return dst
}

func (b *DiskIDsBlock) packRIDs(dst []byte) []byte {
	for _, id := range b.ids {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(id.RID))
	}
	return dst
}

func (b *DiskIDsBlock) packPos(dst []byte) []byte {
	var prev uint64
	for _, pos := range b.pos {
		dst = binary.AppendVarint(dst, int64(pos-prev))
		prev = pos
	}
	return dst
}

type DiskTokenTableBlock struct {
	field   string
	entries []*token.TableEntry
}

func (t DiskTokenTableBlock) pack(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(t.field)))
	dst = append(dst, t.field...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(t.entries)))
	for _, entry := range t.entries {
		dst = entry.Pack(dst)
	}
	return dst
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

func (t *DiskTokensBlock) pack(dst []byte) []byte {
	for _, token := range t.tokens {
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(token)))
		dst = append(dst, token...)
	}
	dst = binary.LittleEndian.AppendUint32(dst, math.MaxUint32)
	return dst
}
