package node

import (
	"fmt"
)

type nodeAnd struct {
	left  Node
	right Node

	leftID  LID
	rightID LID
}

func (n *nodeAnd) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func NewAnd(left, right Node) *nodeAnd {
	node := &nodeAnd{left: left, right: right}
	node.readLeft()
	node.readRight()
	return node
}

func (n *nodeAnd) readLeft() {
	n.leftID = n.left.Next()
}

func (n *nodeAnd) readRight() {
	n.rightID = n.right.Next()
}

func (n *nodeAnd) readLeftGeq(nextID LID) {
	n.leftID = n.left.NextGeq(nextID)
}

func (n *nodeAnd) readRightGeq(nextID LID) {
	n.rightID = n.right.NextGeq(nextID)
}

func (n *nodeAnd) Next() LID {
	for !n.leftID.IsNull() && !n.rightID.IsNull() && n.leftID != n.rightID {
		for !n.rightID.IsNull() && n.leftID.Less(n.rightID) {
			n.readLeftGeq(n.rightID)
		}
		for !n.leftID.IsNull() && n.rightID.Less(n.leftID) {
			n.readRightGeq(n.leftID)
		}
	}
	if n.leftID.IsNull() || n.rightID.IsNull() {
		return NullLID()
	}
	cur := n.leftID
	n.readLeft()
	n.readRight()
	return cur
}

func (n *nodeAnd) NextGeq(nextID LID) LID {
	for {
		for !n.leftID.IsNull() && !n.rightID.IsNull() && !n.leftID.Eq(n.rightID) {
			for !n.rightID.IsNull() && n.leftID.Less(n.rightID) {
				n.readLeftGeq(Max(n.rightID, nextID))
			}
			for !n.leftID.IsNull() && n.rightID.Less(n.leftID) {
				n.readRightGeq(Max(n.leftID, nextID))
			}
		}

		if n.leftID.IsNull() || n.rightID.IsNull() {
			return NullLID()
		}
		cur := n.leftID
		n.readLeft()
		n.readRight()
		if nextID.LessOrEq(cur) {
			return cur
		}
	}
}

type nodeAndBatched struct {
	left  BatchedNode
	right BatchedNode
	asc   bool

	leftBatch  LIDBatch
	rightBatch LIDBatch
}

// NewAndBatched returns a BatchedNode that intersects two batched iterators.
// asc is the LID traversal order for NextBatch / NextBatchGeq (true = low to high).
func NewAndBatched(left, right BatchedNode, asc bool) BatchedNode {
	return &nodeAndBatched{
		left:       left,
		right:      right,
		asc:        asc,
		leftBatch:  EmptyBatch(),
		rightBatch: EmptyBatch(),
	}
}

func (n *nodeAndBatched) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func (n *nodeAndBatched) NextBatch() LIDBatch {
	if n.asc {
		return n.NextBatchGeq(NewAscZeroLID())
	}
	return n.NextBatchGeq(NewDescZeroLID())
}

func (n *nodeAndBatched) NextBatchGeq(nextID LID) LIDBatch {
	for {
		if n.leftBatch.IsEmpty() {
			n.leftBatch = n.left.NextBatchGeq(nextID)
		}
		if n.rightBatch.IsEmpty() {
			n.rightBatch = n.right.NextBatchGeq(nextID)
		}
		if n.leftBatch.IsEmpty() || n.rightBatch.IsEmpty() {
			return EmptyBatch()
		}

		inter, leftResidual, rightResidual := And(n.leftBatch, n.rightBatch, n.asc)
		n.leftBatch = leftResidual
		n.rightBatch = rightResidual

		if !inter.IsEmpty() {
			return inter
		}
	}
}
