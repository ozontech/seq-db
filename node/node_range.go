package node

import "github.com/RoaringBitmap/roaring/v2"

type nodeRange struct {
	maxID LID
	curID LID
}

func (n *nodeRange) String() string {
	return "(RANGE)"
}

func NewRange(minVal, maxVal LID) *nodeRange {
	return &nodeRange{
		curID: minVal,
		maxID: maxVal,
	}
}

func (n *nodeRange) Next() LID {
	if n.maxID.Less(n.curID) {
		return NullLID()
	}
	result := n.curID
	n.curID = n.curID.Inc()
	return result
}

func (n *nodeRange) NextGeq(nextID LID) LID {
	n.curID = Max(n.curID, nextID)
	return n.Next()
}

type nodeRangeBatched struct {
	min, max uint32
	asc      bool
	done     bool
}

func NewRangeBatched(minLID, maxLID uint32, asc bool) BatchedNode {
	return &nodeRangeBatched{min: minLID, max: maxLID, asc: asc}
}

func (n *nodeRangeBatched) String() string {
	return "(RANGE_BATCHED)"
}

func (n *nodeRangeBatched) NextBatch() LIDBatch {
	if n.asc {
		return n.NextBatchGeq(NewAscZeroLID())
	}
	return n.NextBatchGeq(NewDescZeroLID())
}

func (n *nodeRangeBatched) NextBatchGeq(nextID LID) LIDBatch {
	if n.done || n.min > n.max {
		return EmptyBatch()
	}

	raw := nextID.Unpack()
	n.done = true
	if n.asc {
		return newBitmapBatchFromRange(max(n.min, raw), n.max)
	}
	return newBitmapBatchFromRange(n.min, min(n.max, raw))
}

func newBitmapBatchFromRange(minLID, maxLID uint32) LIDBatch {
	bm := roaring.NewBitmap()
	bm.AddRange(uint64(minLID), uint64(maxLID)+1)

	return NewBitmapBatch(bm)
}
