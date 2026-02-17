package node

import "fmt"

type nodeNot struct {
	nodeNAnd
}

func (n *nodeNot) String() string {
	return fmt.Sprintf("(NOT %s)", n.neg.String())
}

func NewNot(child Node, minID, maxID CmpLID) *nodeNot {
	nodeRange := NewRange(minID, maxID)
	nodeNAnd := NewNAnd(child, nodeRange)
	return &nodeNot{nodeNAnd: *(nodeNAnd)}
}
