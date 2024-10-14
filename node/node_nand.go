package node

import "fmt"

type nodeNAnd struct {
	less LessFn

	neg    Node
	negID  uint32
	hasNeg bool

	reg    Node
	regID  uint32
	hasReg bool
}

func (n *nodeNAnd) String() string {
	return fmt.Sprintf("(%s NAND %s)", n.neg.String(), n.reg.String())
}

func NewNAnd(negative, regular Node, reverse bool) *nodeNAnd {
	node := &nodeNAnd{
		less: GetLessFn(reverse),

		neg: negative,
		reg: regular,
	}
	node.readNeg()
	node.readReg()
	return node
}

func (n *nodeNAnd) readNeg() {
	n.negID, n.hasNeg = n.neg.Next()
}

func (n *nodeNAnd) readReg() {
	n.regID, n.hasReg = n.reg.Next()
}

func (n *nodeNAnd) Next() (uint32, bool) {
	for n.hasReg {
		for n.hasNeg && n.less(n.negID, n.regID) {
			n.readNeg()
		}
		if !n.hasNeg || n.negID != n.regID { // i.e. n.negID > regID
			cur := n.regID
			n.readReg()
			return cur, true
		}
		n.readReg()
	}
	return 0, false
}
