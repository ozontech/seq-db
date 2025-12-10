package active2

import (
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

// For compaction
type mergeIterator struct {
	index          *memIndex
	posIDs         int
	posField       int
	posToken       int
	posBlocks      int
	lastFieldToken int
	newLIDs        []uint32
	newPositions   []seq.DocPos
	newBlocks      []int
}

func newIndexIterator(index *memIndex) *mergeIterator {
	x := &mergeIterator{
		index:          index,
		newLIDs:        make([]uint32, 0, len(index.ids)),
		newBlocks:      make([]int, 0, len(index.blocksOffsets)),
		lastFieldToken: int(index.fieldsTokens[string(index.fields[0])].count) - 1,
	}

	field := x.index.fields[x.posField]
	r := x.index.fieldsTokens[string(field)]
	x.lastFieldToken += int(r.count) - 1

	return x
}

func (iq *mergeIterator) ShiftID() bool {
	iq.posIDs++
	if iq.posIDs == len(iq.index.ids) {
		return false
	}
	return true
}

func (iq *mergeIterator) CurrentID() seq.ID {
	return iq.index.ids[iq.posIDs]
}

func (iq *mergeIterator) CurrentPos() seq.DocPos {
	return iq.newPositions[iq.posIDs]
}

func (iq *mergeIterator) ShiftToken() bool {
	iq.posToken++
	if iq.posToken == len(iq.index.tokens) {
		return false
	}
	if iq.posToken > iq.lastFieldToken { // need shift field
		iq.posField++
		field := iq.index.fields[iq.posField]
		r := iq.index.fieldsTokens[string(field)]
		iq.lastFieldToken += int(r.count)
	}
	return true
}

func (iq *mergeIterator) CurrentToken() tokenizer.MetaToken {
	return tokenizer.MetaToken{
		Key:   iq.index.fields[iq.posField],
		Value: iq.index.tokens[iq.posToken],
	}
}

func (iq *mergeIterator) CurrentTokenLIDs() []uint32 {
	src := iq.index.tokenLIDs[iq.posToken]
	dst := make([]uint32, 0, len(src))
	for _, oldLid := range src {
		dst = append(dst, iq.newLIDs[oldLid-1])
	}
	return dst
}

func (iq *mergeIterator) ShiftBlocksOffset() bool {
	iq.posBlocks++
	if iq.posBlocks == len(iq.index.blocksOffsets) {
		return false
	}
	return true
}

func (iq *mergeIterator) CurrentBlocksOffset() uint64 {
	return iq.index.blocksOffsets[iq.posBlocks]
}

func (iq *mergeIterator) AddPos(p seq.DocPos) {
	iq.newPositions = append(iq.newPositions, p)
}

func (iq *mergeIterator) AddNewLID(lid uint32) {
	iq.newLIDs = append(iq.newLIDs, lid)
}

func (iq *mergeIterator) AddNewBlockIndex(blockIndex int) {
	iq.newBlocks = append(iq.newBlocks, blockIndex)
}
