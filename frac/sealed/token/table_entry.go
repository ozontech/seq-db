package token

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

func (t *TableEntry) GetIndexInTokensBlock(tid uint32) int {
	return int(t.StartIndex + tid - t.StartTID)
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
