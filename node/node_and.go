package node

import "fmt"

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

func (n *nodeAnd) Next() (uint32, bool) {
	for n.hasLeft && n.hasRight && n.leftID != n.rightID {
		for n.hasLeft && n.hasRight && n.less(n.leftID, n.rightID) {
			n.readLeft()
		}
		for n.hasLeft && n.hasRight && n.less(n.rightID, n.leftID) {
			n.readRight()
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
