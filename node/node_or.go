package node

import (
	"fmt"
)

type nodeOr struct {
	left  Node
	right Node

	leftID  LID
	rightID LID
}

func (n *nodeOr) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func NewOr(left, right Node) *nodeOr {
	n := &nodeOr{left: left, right: right}
	n.readLeft()
	n.readRight()
	return n
}

func (n *nodeOr) readLeft() {
	n.leftID = n.left.Next()
}

func (n *nodeOr) readRight() {
	n.rightID = n.right.Next()
}

func (n *nodeOr) readLeftGeq(nextID LID) {
	n.leftID = n.left.NextGeq(nextID)
}

func (n *nodeOr) readRightGeq(nextID LID) {
	n.rightID = n.right.NextGeq(nextID)
}

func (n *nodeOr) Next() LID {
	if n.leftID.IsNull() && n.rightID.IsNull() {
		return n.leftID
	}

	if n.leftID.Less(n.rightID) {
		cur := n.leftID
		n.readLeft()
		return cur
	}
	if n.rightID.Less(n.leftID) {
		cur := n.rightID
		n.readRight()
		return cur
	}
	cur := n.leftID
	n.readLeft()
	n.readRight()
	return cur
}

func (n *nodeOr) NextGeq(nextID LID) LID {
	// Fast path: if we at least left or right and there is nothing to skip, then choose lowest and return.
	minID := Min(n.leftID, n.rightID)
	if nextID.LessOrEq(minID) {
		return n.Next()
	}

	if n.leftID.Less(nextID) {
		n.readLeftGeq(nextID)
	}
	if n.rightID.Less(nextID) {
		n.readRightGeq(nextID)
	}

	return n.Next()
}

type nodeOrAgg struct {
	left  Sourced
	right Sourced

	leftID     LID
	leftSource uint32

	rightID     LID
	rightSource uint32
}

func (n *nodeOrAgg) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func NewNodeOrAgg(left, right Sourced) Sourced {
	n := &nodeOrAgg{left: left, right: right}
	n.readLeft()
	n.readRight()
	return n
}

func (n *nodeOrAgg) readLeft() {
	n.leftID, n.leftSource = n.left.NextSourced()
}

func (n *nodeOrAgg) readRight() {
	n.rightID, n.rightSource = n.right.NextSourced()
}

func (n *nodeOrAgg) readLeftGeq(nextID LID) {
	n.leftID, n.leftSource = n.left.NextSourcedGeq(nextID)
}

func (n *nodeOrAgg) readRightGeq(nextID LID) {
	n.rightID, n.rightSource = n.right.NextSourcedGeq(nextID)
}

func (n *nodeOrAgg) NextSourced() (LID, uint32) {
	if n.leftID.IsNull() && n.rightID.IsNull() {
		return n.leftID, 0
	}
	if n.leftID.Less(n.rightID) {
		cur := n.leftID
		curSource := n.leftSource
		n.readLeft()
		return cur, curSource
	}
	cur := n.rightID
	curSource := n.rightSource
	n.readRight()
	return cur, curSource
}

func (n *nodeOrAgg) NextSourcedGeq(nextID LID) (LID, uint32) {
	// Fast path: if we at least left or right and there is nothing to skip, then choose lowest and return.
	minID := Min(n.leftID, n.rightID)
	if nextID.LessOrEq(minID) {
		if n.leftID.Less(n.rightID) {
			cur := n.leftID
			curSource := n.leftSource
			n.readLeft()
			return cur, curSource
		} else {
			// we don't need deduplication
			cur := n.rightID
			curSource := n.rightSource
			n.readRight()
			return cur, curSource
		}
	}

	if n.leftID.Less(nextID) {
		n.readLeftGeq(nextID)
	}
	if n.rightID.Less(nextID) {
		n.readRightGeq(nextID)
	}

	return n.NextSourced()
}

type nodeOrBatched struct {
	left  BatchedNode
	right BatchedNode
	asc   bool

	leftBatch  LIDBatch
	rightBatch LIDBatch
	leftDone   bool
	rightDone  bool
}

// NewOrBatched returns a BatchedNode that unions two batched iterators.
// asc is the LID traversal order for NextBatch / NextBatchGeq (true = low to high).
func NewOrBatched(left, right BatchedNode, asc bool) BatchedNode {
	return &nodeOrBatched{
		left:       left,
		right:      right,
		asc:        asc,
		leftBatch:  EmptyBatch(),
		rightBatch: EmptyBatch(),
	}
}

func (n *nodeOrBatched) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func (n *nodeOrBatched) NextBatch() LIDBatch {
	if n.asc {
		return n.NextBatchGeq(NewAscZeroLID())
	}
	return n.NextBatchGeq(NewDescZeroLID())
}

func (n *nodeOrBatched) NextBatchGeq(nextID LID) LIDBatch {
	for {
		if n.leftBatch.IsEmpty() && !n.leftDone {
			n.leftBatch = n.left.NextBatchGeq(nextID)
			n.leftDone = n.leftBatch.IsEmpty()
		}

		if n.rightBatch.IsEmpty() && !n.rightDone {
			n.rightBatch = n.right.NextBatchGeq(nextID)
			n.rightDone = n.rightBatch.IsEmpty()
		}

		if n.leftDone && n.rightDone && n.leftBatch.IsEmpty() && n.rightBatch.IsEmpty() {
			return EmptyBatch()
		}

		out, leftRes, rightRes := Or(n.leftBatch, n.rightBatch, n.asc)
		n.leftBatch = leftRes
		n.rightBatch = rightRes

		if !out.IsEmpty() {
			return out
		}
	}
}

type nodeOrBatchedMulti struct {
	children []BatchedNode
	asc      bool

	batches []LIDBatch
	done    []bool
}

func NewOrBatchedMulti(children []BatchedNode, asc bool) BatchedNode {
	if len(children) == 0 {
		return EmptyBatched()
	}
	if len(children) == 1 {
		return children[0]
	}
	batches := make([]LIDBatch, len(children))
	for i := range batches {
		batches[i] = EmptyBatch()
	}
	return &nodeOrBatchedMulti{
		children: children,
		asc:      asc,
		batches:  batches,
		done:     make([]bool, len(children)),
	}
}

func (n *nodeOrBatchedMulti) String() string {
	return "OR_MULTI_BATCHED"
}

func (n *nodeOrBatchedMulti) NextBatch() LIDBatch {
	if n.asc {
		return n.NextBatchGeq(NewAscZeroLID())
	}
	return n.NextBatchGeq(NewDescZeroLID())
}

func (n *nodeOrBatchedMulti) NextBatchGeq(nextID LID) LIDBatch {
	for {
		active := 0
		for i := range n.children {
			if n.batches[i].IsEmpty() && !n.done[i] {
				n.batches[i] = n.children[i].NextBatchGeq(nextID)
				n.done[i] = n.batches[i].IsEmpty()
			}
			if !n.batches[i].IsEmpty() {
				active++
			}
		}

		if active == 0 {
			return EmptyBatch()
		}

		out, residuals := OrMulti(n.batches, n.asc)
		n.batches = residuals

		if !out.IsEmpty() {
			return out
		}
	}
}
