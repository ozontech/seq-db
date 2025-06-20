package token

import (
	"encoding/binary"

	"go.uber.org/zap/zapcore"
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

func (t *TableEntry) Pack(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, t.StartTID)
	dst = binary.LittleEndian.AppendUint32(dst, t.ValCount)
	dst = binary.LittleEndian.AppendUint32(dst, t.StartIndex) // todo: it seems we do not need to store this field - we can calculate it from ValCount while reading
	dst = binary.LittleEndian.AppendUint32(dst, t.BlockIndex)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(t.MinVal)))
	dst = append(dst, t.MinVal...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(t.MaxVal)))
	dst = append(dst, t.MaxVal...)
	return dst
}

func (t *TableEntry) getIndexInTokensBlock(tid uint32) uint32 {
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
