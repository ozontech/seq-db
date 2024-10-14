package node

import "fmt"

type nodeNot struct {
	nodeNAnd
}

func (n *nodeNot) String() string {
	return fmt.Sprintf("(NOT %s)", n.neg.String())
}

func NewNot(child Node, minVal, maxVal uint32, reverse bool) *nodeNot {
	nodeRange := NewRange(minVal, maxVal, reverse)
	nodeNAnd := NewNAnd(child, nodeRange, reverse)
	return &nodeNot{nodeNAnd: *(nodeNAnd)}
}
