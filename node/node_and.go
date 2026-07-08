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
	desc  bool

	leftBatch  LIDBatch
	rightBatch LIDBatch
}

// NewAndBatched returns a BatchedNode that intersects two batched iterators.
// desc is the document traversal order for NextBatch / NextBatchGeq.
func NewAndBatched(left, right BatchedNode, desc bool) BatchedNode {
	return &nodeAndBatched{
		left:       left,
		right:      right,
		desc:       desc,
		leftBatch:  EmptyBatch(),
		rightBatch: EmptyBatch(),
	}
}

func (n *nodeAndBatched) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func (n *nodeAndBatched) NextBatch(need int) LIDBatch {
	if n.desc {
		return n.NextBatchGeq(need, NewDescZeroLID())
	}
	return n.NextBatchGeq(need, NewAscZeroLID())
}

func (n *nodeAndBatched) NextBatchGeq(need int, minLID LID) LIDBatch {
	for {
		if n.leftBatch.IsEmpty() {
			n.leftBatch = n.left.NextBatchGeq(need, minLID)
		}
		if n.rightBatch.IsEmpty() {
			n.rightBatch = n.right.NextBatchGeq(need, minLID)
		}
		if n.leftBatch.IsEmpty() || n.rightBatch.IsEmpty() {
			return EmptyBatch()
		}

		inter, leftResidual, rightResidual := And(n.leftBatch, n.rightBatch, n.desc)
		n.leftBatch = leftResidual
		n.rightBatch = rightResidual

		if !inter.IsEmpty() {
			return inter
		}
	}
}
