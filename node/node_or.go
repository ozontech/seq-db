package node

import "fmt"

type nodeOr struct {
	less LessFn

	left  singleIter
	right singleIter

	leftID   uint32
	hasLeft  bool
	rightID  uint32
	hasRight bool
}

func (n *nodeOr) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.node.String(), n.right.node.String())
}

func NewOr(left, right Node, reverse bool) *nodeOr {
	n := &nodeOr{
		less:  GetLessFn(reverse),
		left:  singleIter{node: left},
		right: singleIter{node: right},
	}
	n.leftID, n.hasLeft = n.left.next()
	n.rightID, n.hasRight = n.right.next()
	return n
}

func (n *nodeOr) Next(limit uint32) []uint32 {
	// TODO support batching here
	if !n.hasLeft && !n.hasRight {
		return nil
	}

	var cur uint32
	if n.hasLeft && (!n.hasRight || n.less(n.leftID, n.rightID)) {
		cur = n.leftID
		n.leftID, n.hasLeft = n.left.next()
	} else if n.hasRight && (!n.hasLeft || n.less(n.rightID, n.leftID)) {
		cur = n.rightID
		n.rightID, n.hasRight = n.right.next()
	} else {
		cur = n.leftID
		n.leftID, n.hasLeft = n.left.next()
		n.rightID, n.hasRight = n.right.next()
	}

	return []uint32{cur}
}

type nodeOrAgg struct {
	left  Sourced
	right Sourced

	leftID     uint32
	leftSource uint32
	hasLeft    bool

	rightID     uint32
	rightSource uint32
	hasRight    bool

	less LessFn
}

func (n *nodeOrAgg) String() string {
	return fmt.Sprintf("(%s OR %s)", n.left.String(), n.right.String())
}

func NewNodeOrAgg(left, right Sourced, reverse bool) Sourced {
	n := &nodeOrAgg{
		left:  left,
		right: right,
		less:  GetLessFn(reverse),
	}
	n.readLeft()
	n.readRight()
	return n
}

func (n *nodeOrAgg) readLeft() {
	n.leftID, n.leftSource, n.hasLeft = n.left.NextSourced()
}

func (n *nodeOrAgg) readRight() {
	n.rightID, n.rightSource, n.hasRight = n.right.NextSourced()
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
