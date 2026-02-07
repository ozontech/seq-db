package node

import "fmt"

type nodeNAnd struct {
	less LessFn

	neg    singleIter
	negID  uint32
	hasNeg bool

	reg    singleIter
	regID  uint32
	hasReg bool
}

func (n *nodeNAnd) String() string {
	return fmt.Sprintf("(%s NAND %s)", n.neg.node.String(), n.reg.node.String())
}

func NewNAnd(negative, regular Node, reverse bool) *nodeNAnd {
	node := &nodeNAnd{
		less: GetLessFn(reverse),
		neg:  singleIter{node: negative},
		reg:  singleIter{node: regular},
	}
	node.negID, node.hasNeg = node.neg.next()
	node.regID, node.hasReg = node.reg.next()
	return node
}

func (n *nodeNAnd) Next(limit uint32) []uint32 {
	// TODO support batching?
	for n.hasReg {
		for n.hasNeg && n.less(n.negID, n.regID) {
			n.negID, n.hasNeg = n.neg.next()
		}
		if !n.hasNeg || n.negID != n.regID {
			cur := n.regID
			n.regID, n.hasReg = n.reg.next()
			return []uint32{cur}
		}
		n.regID, n.hasReg = n.reg.next()
	}
	return nil
}

func (n *nodeNAnd) NextGeq(minLID uint32, limit uint32) []uint32 {
	return n.Next(limit)
}
