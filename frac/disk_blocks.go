package frac

import (
	"math"

	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/packer"
)

type DiskInfoBlock struct {
	info *Info
}

func (b *DiskInfoBlock) pack(p *packer.BytesPacker) {
	p.PutBytes([]byte(seqDBMagic))
	p.PutBytes(b.info.Save())
}

type DiskPositionsBlock struct {
	totalIDs uint32
	blocks   []uint64
}

func (b *DiskPositionsBlock) pack(p *packer.BytesPacker) {
	p.PutUint32(uint32(len(b.blocks)))
	p.PutUint32(b.totalIDs)

	var prev uint64
	for _, pos := range b.blocks {
		p.PutVarint(int64(pos - prev))
		prev = pos
	}
}

type DiskTokenTableBlock struct {
	field   string
	entries []*token.TableEntry
}

func (t DiskTokenTableBlock) pack(p *packer.BytesPacker) {
	p.PutStringWithSize(t.field)
	p.PutUint32(uint32(len(t.entries)))
	for _, entry := range t.entries {
		entry.Pack(p)
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
