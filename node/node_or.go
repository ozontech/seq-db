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
