package node

import "fmt"

type nodeOr struct {
	less     LessFn
	left     Node
	right    Node
	leftID   uint32
	hasLeft  bool
	rightID  uint32
	hasRight bool
}

func (n *nodeOr) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func NewOr(left, right Node, reverse bool) *nodeOr {
	n := &nodeOr{
		less:  GetLessFn(reverse),
		left:  left,
		right: right,
	}
	n.leftID, n.hasLeft = n.left.Next()
	n.rightID, n.hasRight = n.right.Next()
	return n
}

func (n *nodeOr) Next() (uint32, bool) {
	if !n.hasLeft && !n.hasRight {
		return 0, false
	}

	if n.hasLeft && (!n.hasRight || n.less(n.leftID, n.rightID)) {
		cur := n.leftID
		n.readLeft()
		return cur, true
	}

	if n.hasRight && (!n.hasLeft || n.less(n.rightID, n.leftID)) {
		cur := n.rightID
		n.readRight()
		return cur, true
	}

	cur := n.leftID
	n.readLeft()
	n.readRight()

	return cur, true
}

func (n *nodeOr) NextGeq(minLID uint32) (uint32, bool) {
	if !n.hasLeft && !n.hasRight {
		return 0, false
	}

	if n.hasLeft && (!n.hasRight || n.less(n.leftID, n.rightID)) {
		cur := n.leftID
		n.readLeftGeq(minLID)
		return cur, true
	}

	if n.hasRight && (!n.hasLeft || n.less(n.rightID, n.leftID)) {
		cur := n.rightID
		n.readRightGeq(minLID)
		return cur, true
	}

	cur := n.leftID
	n.readLeftGeq(minLID)
	n.readRightGeq(minLID)

	return cur, true
}

func (n *nodeOr) readLeft() {
	n.leftID, n.hasLeft = n.left.Next()
}

func (n *nodeOr) readRight() {
	n.rightID, n.hasRight = n.right.Next()
}

func (n *nodeOr) readLeftGeq(minLID uint32) {
	n.leftID, n.hasLeft = n.left.NextGeq(minLID)
}

func (n *nodeOr) readRightGeq(minLID uint32) {
	n.rightID, n.hasRight = n.right.NextGeq(minLID)
}

type nodeOrAgg struct {
	less        LessFn
	left        Sourced
	right       Sourced
	leftID      uint32
	leftSource  uint32
	hasLeft     bool
	rightID     uint32
	rightSource uint32
	hasRight    bool
}

func (n *nodeOrAgg) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func NewNodeOrAgg(left, right Sourced, reverse bool) Sourced {
	no := &nodeOrAgg{
		less:  GetLessFn(reverse),
		left:  left,
		right: right,
	}
	no.leftID, no.leftSource, no.hasLeft = no.left.NextSourced()
	no.rightID, no.rightSource, no.hasRight = no.right.NextSourced()
	return no
}

func (n *nodeOrAgg) readLeft() {
	n.leftID, n.leftSource, n.hasLeft = n.left.NextSourced()
}

func (n *nodeOrAgg) readRight() {
	n.rightID, n.rightSource, n.hasRight = n.right.NextSourced()
}

func (n *nodeOrAgg) readLeftGeq(minLID uint32) {
	n.leftID, n.leftSource, n.hasLeft = n.left.NextSourcedGeq(minLID)
}

func (n *nodeOrAgg) readRightGeq(minLID uint32) {
	n.rightID, n.rightSource, n.hasRight = n.right.NextSourcedGeq(minLID)
}

func (n *nodeOrAgg) NextSourced() (uint32, uint32, bool) {
	if !n.hasLeft && !n.hasRight {
		return 0, 0, false
	}

	if n.hasLeft && (!n.hasRight || n.less(n.leftID, n.rightID)) {
		cur := n.leftID
		curSource := n.leftSource
		n.readLeft()

		return cur, curSource, true
	}

	// we don't need deduplication
	cur := n.rightID
	curSource := n.rightSource
	n.readRight()

	return cur, curSource, true
}

func (n *nodeOrAgg) NextSourcedGeq(minLID uint32) (uint32, uint32, bool) {
	if !n.hasLeft && !n.hasRight {
		return 0, 0, false
	}

	var cur uint32
	var curSource uint32

	for cur < minLID && (n.hasRight || n.hasLeft) {
		if n.hasLeft && (!n.hasRight || n.less(n.leftID, n.rightID)) {
			cur = n.leftID
			curSource = n.leftSource
			n.readLeftGeq(minLID)

			return cur, curSource, true
		}

		// we don't need deduplication
		cur = n.rightID
		curSource = n.rightSource
		n.readRightGeq(minLID)
	}

	if cur >= minLID {
		return cur, curSource, true
	} else {
		return 0, 0, false
	}
}
