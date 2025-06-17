package token

import (
	"github.com/ozontech/seq-db/packer"
)

// TableEntry describes token.Block metadata: what TID and tokens it contains and etc.
// One token.Block can cover multiple instances of token.TableEntry
type TableEntry struct {
	StartIndex uint32 // number of tokens in block before this TokenEntry
	StartTID   uint32 // first TID of TableEntry
	BlockIndex uint32 // sequence number of the physical block of tokens in the file
	ValCount   uint32

	MinVal string // only saved for the first entry in block
	MaxVal string
}

func (t *TableEntry) Pack(p *packer.BytesPacker) {
	p.PutUint32(t.StartTID)
	p.PutUint32(t.ValCount)
	p.PutUint32(t.StartIndex) // todo: it seems we do not need to store this field - we can calculate it from ValCount while reading
	p.PutUint32(t.BlockIndex)
	p.PutStringWithSize(t.MinVal)
	p.PutStringWithSize(t.MaxVal)
}

func (t *TableEntry) GetIndexInTokensBlock(tid uint32) uint32 {
	return t.StartIndex + tid - t.StartTID
}

func (t *TableEntry) getLastTID() uint32 {
	return t.StartTID + t.ValCount - 1
}

func (t *TableEntry) checkTIDInBlock(tid uint32) bool {
	if tid < t.StartTID {
		return false
	}

	if tid > t.getLastTID() {
		return false
	}

	return true
}
