package frac

import (
	"encoding/binary"
	"math"

	"github.com/ozontech/seq-db/frac/ids"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/seq"
)

type idsBlock struct {
	mids   ids.BlockMIDs
	rids   ids.BlockRIDs
	params ids.BlockParams
}

func (idsBlock) GetExtForRegistry() (uint64, uint64) {
	return 0, 0
}

func (idsBlock) GetMinID() seq.ID {
	return seq.ID{}
}

// func (b *Block) GetMinID() seq.ID {
// 	return b.IDs[len(b.IDs)-1]
// }

// func (b *Block) GetExtForRegistry() (uint64, uint64) {
// 	last := b.GetMinID()
// 	return uint64(last.MID), uint64(last.RID)
// }

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
