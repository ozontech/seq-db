package token

import "github.com/ozontech/seq-db/util"

// TableEntry is part of token.Table and points to a fragment of token.Block
type TableEntry struct {
	StartIndex uint32 // offset from the beginning of the block to the first token pointed to by the TableEntry
	StartTID   uint32 // first TID of TableEntry
	BlockIndex uint32 // sequence number of the physical block of tokens in the file
	ValCount   uint32

	MinVal string // only saved for the first entry in block
	MaxVal string

	Letters util.LettersBitset // case-insensitive English letters present in entry tokens
}

func (t *TableEntry) GetIndexInTokensBlock(tid uint32) int {
	return int(t.StartIndex + (tid - t.StartTID))
}

func (t *TableEntry) GetLastTID() uint32 {
	return t.StartTID + t.ValCount - 1
}

func (t *TableEntry) narrowIndexes(firstTID, lastTID uint32) (int, int) {
	tidStart := firstTID
	if t.StartTID > tidStart {
		tidStart = t.StartTID
	}
	tidEnd := lastTID
	if entryLastTID := t.GetLastTID(); entryLastTID < tidEnd {
		tidEnd = entryLastTID
	}

	firstIndex := t.GetIndexInTokensBlock(tidStart)
	lastIndex := t.GetIndexInTokensBlock(tidEnd)
	return firstIndex, lastIndex
}

func (t *TableEntry) checkTIDsInBlock(firstTID, lastTID uint32) bool {
	if lastTID < t.StartTID {
		return false
	}

	if firstTID > t.GetLastTID() {
		return false
	}

	return true
}

func (t *TableEntry) checkTIDInBlock(tid uint32) bool {
	if tid < t.StartTID {
		return false
	}

	if tid > t.GetLastTID() {
		return false
	}

	return true
}
