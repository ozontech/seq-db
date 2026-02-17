package node

import "fmt"

type nodeOr struct {
	left  Node
	right Node

	leftID  CmpLID
	rightID CmpLID
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

func (n *nodeOr) Next() CmpLID {
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

type nodeOrAgg struct {
	reverse bool

	left  Sourced
	right Sourced

	leftID     CmpLID
	leftSource uint32

	rightID     CmpLID
	rightSource uint32
}

func (n *nodeOrAgg) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func NewNodeOrAgg(left, right Sourced, reverse bool) Sourced {
	n := &nodeOrAgg{reverse: reverse, left: left, right: right}
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

func (n *nodeOrAgg) NextSourced() (CmpLID, uint32) {
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
