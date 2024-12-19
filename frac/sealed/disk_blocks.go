package sealed

import (
	"math"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/seq"
)

const seqDBMagic = "SEQM"

type DiskInfoBlock struct {
	Info frac.Info
}

func (b *DiskInfoBlock) Pack(p *packer.BytesPacker) {
	p.PutBytes([]byte(seqDBMagic))
	p.PutBytes(b.Info.Save())
}

type DiskPositionsBlock struct {
	TotalIDs uint32
	Blocks   []uint64
}

func (b *DiskPositionsBlock) Pack(p *packer.BytesPacker) {
	p.PutUint32(uint32(len(b.Blocks)))
	p.PutUint32(b.TotalIDs)

	var prev uint64
	for _, pos := range b.Blocks {
		p.PutVarint(int64(pos - prev))
		prev = pos
	}
}

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

func (b *DiskIDsBlock) PackMIDs(p *packer.BytesPacker) {
	var mid, prev uint64
	for _, id := range b.IDs {
		mid = uint64(id.MID)
		p.PutVarint(int64(mid - prev))
		prev = mid
	}
}

func (b *DiskIDsBlock) PackRIDs(p *packer.BytesPacker) {
	for _, id := range b.IDs {
		p.PutUint64(uint64(id.RID))
	}
}

func (b *DiskIDsBlock) PackPos(p *packer.BytesPacker) {
	var prev uint64
	for _, pos := range b.Pos {
		p.PutVarint(int64(pos - prev))
		prev = pos
	}
}

type DiskTokenTableBlock struct {
	Field   string
	Entries []*token.TableEntry
}

func (t DiskTokenTableBlock) Pack(p *packer.BytesPacker) {
	p.PutStringWithSize(t.Field)
	p.PutUint32(uint32(len(t.Entries)))
	for _, entry := range t.Entries {
		entry.Pack(p)
	}
}

type DiskTokensBlock struct {
	Field            string
	IsStartOfField   bool
	TotalSizeOfField int
	StartTID         uint32
	Tokens           [][]byte
}

func (t *DiskTokensBlock) CreateTokenTableEntry(startIndex, blockIndex uint32) *token.TableEntry {
	size := len(t.Tokens)
	return &token.TableEntry{
		StartIndex: startIndex,
		StartTID:   t.StartTID,
		ValCount:   uint32(size),
		BlockIndex: blockIndex,
		MaxVal:     string(t.Tokens[size-1]),
	}
}

func (t *DiskTokensBlock) Pack(p *packer.BytesPacker) {
	for _, token := range t.Tokens {
		p.PutUint32(uint32(len(token)))
		p.PutBytes(token)
	}
	p.PutUint32(math.MaxUint32)
}
