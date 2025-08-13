package frac

import (
	"encoding/binary"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
)

type idsBlock struct {
	mids   seqids.BlockMIDs
	rids   seqids.BlockRIDs
	params seqids.BlockParams
}

func (b idsBlock) GetMinID() seq.ID {
	last := len(b.mids.Values) - 1
	return seq.ID{
		MID: seq.MID(b.mids.Values[last]),
		RID: seq.RID(b.rids.Values[last]),
	}
}

func (b idsBlock) GetExtForRegistry() (uint64, uint64) {
	last := b.GetMinID()
	return uint64(last.MID), uint64(last.RID)
}

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

type tokensBlock struct {
	field            string
	isStartOfField   bool
	totalSizeOfField int
	startTID         uint32
	payload          token.Block
}

func (t *tokensBlock) createTokenTableEntry(startIndex, blockIndex uint32) *token.TableEntry {
	size := len(t.payload.Offsets)
	return &token.TableEntry{
		StartIndex: startIndex,
		StartTID:   t.startTID,
		ValCount:   uint32(size),
		BlockIndex: blockIndex,
		MaxVal:     string(t.payload.GetToken(size - 1)),
	}
}
