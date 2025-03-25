package active2

import (
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

// For compaction
type indexIterator struct {
	index          *Index
	posIDs         int
	posField       int
	posToken       int
	posBlocks      int
	lastFieldToken int
	newLIDs        []uint32
	newBlocks      []int
}

func newIndexIterator(index *Index) indexIterator {
	return indexIterator{
		index:          index,
		newLIDs:        make([]uint32, len(index.ids)),
		newBlocks:      make([]int, len(index.blocksOffsets)),
		lastFieldToken: index.fieldsTokens[string(index.fields[0])].count - 1,
	}
}

func (iq *indexIterator) ShiftID() bool {
	iq.posIDs++
	if iq.posIDs == len(iq.index.ids) {
		return false
	}
	return true
}

func (iq *indexIterator) CurrentID() seq.ID {
	return iq.index.ids[iq.posIDs]
}

func (iq *indexIterator) ShiftToken() bool {
	iq.posToken++
	if iq.posToken == len(iq.index.tokens) {
		return false
	}
	if iq.posToken > iq.lastFieldToken { // need shift field
		iq.posField++
		field := iq.index.fields[iq.posField]
		r := iq.index.fieldsTokens[string(field)]
		iq.lastFieldToken += r.count - 1
	}
	return true
}

func (iq *indexIterator) CurrentToken() frac.MetaToken {
	return frac.MetaToken{
		Key:   iq.index.fields[iq.posField],
		Value: iq.index.tokens[iq.posToken],
	}
}

func (iq *indexIterator) CurrentTokenLIDs() []uint32 {
	src := iq.index.tokenLIDs[iq.posToken]
	dst := make([]uint32, 0, len(src))
	for _, oldLid := range src {
		dst = append(dst, iq.newLIDs[oldLid-1]+1)
	}
	return dst
}

func (iq *indexIterator) ShiftBlocksOffset() bool {
	iq.posBlocks++
	if iq.posBlocks == len(iq.index.blocksOffsets) {
		return false
	}
	return true
}

func (iq *indexIterator) CurrentBlocksOffset() uint64 {
	return iq.index.blocksOffsets[iq.posBlocks]
}

func (iq *indexIterator) AddNewLID(lid uint32) {
	iq.newLIDs = append(iq.newLIDs, lid)
}

func (iq *indexIterator) AddNewBlockIndex(blockIndex int) {
	iq.newBlocks = append(iq.newBlocks, blockIndex)
}

func (iq *indexIterator) RepackDocPositions(dst map[seq.ID]seq.DocPos) {
	for id, docPos := range iq.index.positions {
		oldBlockIndex, docOffset := docPos.Unpack()
		newBlockIndex := uint32(iq.newBlocks[oldBlockIndex])
		dst[id] = seq.PackDocPos(newBlockIndex, docOffset)
	}
}
