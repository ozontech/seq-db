package node

import "fmt"

type nodeNAnd struct {
	neg Node
	reg Node

	negCmp CmpLID
	regCmp CmpLID
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
	n.negCmp = n.neg.Next()
}

func (n *nodeNAnd) readReg() {
	n.regCmp = n.reg.Next()
}

func (n *nodeNAnd) Next() CmpLID {
	for !n.regCmp.IsNull() {
		for !n.negCmp.IsNull() && n.negCmp.Less(n.regCmp) {
			n.readNeg()
		}
		if n.negCmp.IsNull() || !n.negCmp.Eq(n.regCmp) {
			cur := n.regCmp
			n.readReg()
			return cur
		}
		n.readReg()
	}
	return NullCmpLID()
}
