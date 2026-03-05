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

func (n *nodeAnd) Next() LID {
	for !n.leftID.IsNull() && !n.rightID.IsNull() && !n.leftID.Eq(n.rightID) {
		for !n.rightID.IsNull() && n.leftID.Less(n.rightID) {
			n.readLeft()
		}
		for !n.leftID.IsNull() && n.rightID.Less(n.leftID) {
			n.readRight()
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
