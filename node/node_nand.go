package node

import "fmt"

type nodeNAnd struct {
	neg Node
	reg Node

	negID CmpLID
	regID CmpLID
}

func (n *nodeNAnd) String() string {
	return fmt.Sprintf("(%s NAND %s)", n.neg.String(), n.reg.String())
}

func NewNAnd(negative, regular Node) *nodeNAnd {
	node := &nodeNAnd{neg: negative, reg: regular}
	node.readNeg()
	node.readReg()
	return node
}

func (n *nodeNAnd) readNeg() {
	n.negID = n.neg.Next()
}

func (n *nodeNAnd) readReg() {
	n.regID = n.reg.Next()
}

func (n *nodeNAnd) Next() CmpLID {
	for !n.regID.IsNull() {
		for !n.negID.IsNull() && n.negID.Less(n.regID) {
			n.readNeg()
		}
		if n.negID.IsNull() || !n.negID.Eq(n.regID) {
			cur := n.regID
			n.readReg()
			return cur
		}
		n.readReg()
	}
	return NullCmpLID()
}
