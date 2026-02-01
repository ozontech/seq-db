package node

import (
	"fmt"
)

type nodeAnd struct {
	less LessFn

	left  Node
	right Node

	leftID   uint32
	hasLeft  bool
	rightID  uint32
	hasRight bool
}

func (n *nodeAnd) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func NewAnd(left, right Node, reverse bool) *nodeAnd {
	node := &nodeAnd{
		less: GetLessFn(reverse),

		left:  left,
		right: right,
	}
	node.readLeft()
	node.readRight()
	return node
}

func (n *nodeAnd) readLeft() {
	n.leftID, n.hasLeft = n.left.Next()
}

func (n *nodeAnd) readRight() {
	n.rightID, n.hasRight = n.right.Next()
}

func (n *nodeAnd) readLeftGeq(minLID uint32) {
	n.leftID, n.hasLeft = n.left.NextGeq(minLID)
}

func (n *nodeAnd) readRightGeq(minLID uint32) {
	n.rightID, n.hasRight = n.right.NextGeq(minLID)
}

func (n *nodeAnd) Next() (uint32, bool) {
	for n.hasLeft && n.hasRight && n.leftID != n.rightID {
		for n.hasLeft && n.hasRight && n.less(n.leftID, n.rightID) {
			n.readLeftGeq(n.rightID)
		}
		for n.hasLeft && n.hasRight && n.less(n.rightID, n.leftID) {
			n.readRightGeq(n.leftID)
		}
	}
	if !n.hasLeft || !n.hasRight {
		return 0, false
	}
	cur := n.leftID
	n.readLeft()
	n.readRight()
	return cur, true
}

func (n *nodeAnd) NextGeq(minLID uint32) (uint32, bool) {
	for n.hasLeft && n.hasRight && n.leftID != n.rightID {
		for n.hasLeft && n.hasRight && n.less(n.leftID, n.rightID) {
			n.readLeftGeq(max(minLID, n.rightID))
		}
		for n.hasLeft && n.hasRight && n.less(n.rightID, n.leftID) {
			n.readRightGeq(max(minLID, n.leftID))
		}
	}
	if !n.hasLeft || !n.hasRight {
		return 0, false
	}
	cur := n.leftID
	n.readLeft()
	n.readRight()
	return cur, true
}
