package token

import (
	"go.uber.org/zap/zapcore"

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

func (t *TableEntry) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint32("start_index", t.StartIndex)
	enc.AddUint32("start_tid", t.StartTID)
	enc.AddUint32("val_count", t.ValCount)
	enc.AddUint32("block_index", t.BlockIndex)
	enc.AddString("max_val", t.MaxVal)
	return nil
}

func (t *TableEntry) Pack(p *packer.BytesPacker) {
	p.PutUint32(t.StartTID)
	p.PutUint32(t.ValCount)
	p.PutUint32(t.StartIndex) // todo: it seems we do not need to store this field - we can calculate it from ValCount while reading
	p.PutUint32(t.BlockIndex)
	p.PutStringWithSize(t.MinVal)
	p.PutStringWithSize(t.MaxVal)
}

func (t *TableEntry) getIndexInTokensBlock(tid uint32) uint32 {
	return t.StartIndex + tid - t.StartTID
}

func (t *TableEntry) GetLastTID() uint32 {
	return t.StartTID + t.ValCount - 1
}
