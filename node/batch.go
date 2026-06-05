package node

import (
	"slices"
)

// LIDBatch represents a batch of LIDs. lids are stored as uint32 slice and sorted in ascending order regardless of doc order.
// This allows to avoid copying and use reference to LID blocks data.
// Such batches are also logically immutable - we can't append or delete from them, only union or intersect. But we can zero out (reset) them.
type LIDBatch struct {
	lids []uint32
	desc bool // if doc order is DESC (default order)
}

// NewDescBatch creates a batch of lids for DESC docs order
func NewDescBatch(lids []uint32) LIDBatch {
	return LIDBatch{
		lids: lids,
		desc: true,
	}
}

// NewAscBatch creates a batch of lids for ASC docs order
func NewAscBatch(lids []uint32) LIDBatch {
	return LIDBatch{
		lids: lids,
		desc: false,
	}
}

func (b LIDBatch) Len() int {
	return len(b.lids)
}

func (b LIDBatch) LIDs(out []LID) []LID {
	if b.desc {
		for _, lid := range b.lids {
			out = append(out, NewDescLID(lid))
		}
	} else {
		for _, lid := range slices.Backward(b.lids) {
			out = append(out, NewAscLID(lid))
		}
	}

	return out
}

func (b LIDBatch) Reset() LIDBatch {
	return LIDBatch{
		lids: b.lids[:0],
		desc: b.desc,
	}
}
