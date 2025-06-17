package frac

import (
	"math"

	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/seq"
)

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

func (b *DiskIDsBlock) packMIDs(p *packer.BytesPacker) {
	var mid, prev uint64
	for _, id := range b.ids {
		mid = uint64(id.MID)
		p.PutVarint(int64(mid - prev))
		prev = mid
	}
}

func (b *DiskIDsBlock) packRIDs(p *packer.BytesPacker) {
	for _, id := range b.ids {
		p.PutUint64(uint64(id.RID))
	}
}

func (b *DiskIDsBlock) packPos(p *packer.BytesPacker) {
	var prev uint64
	for _, pos := range b.pos {
		p.PutVarint(int64(pos - prev))
		prev = pos
	}
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

func (t *DiskTokensBlock) pack(p *packer.BytesPacker) {
	for _, token := range t.tokens {
		p.PutUint32(uint32(len(token)))
		p.PutBytes(token)
	}
	p.PutUint32(math.MaxUint32)
}
