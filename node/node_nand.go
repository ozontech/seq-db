package node

import "fmt"

type nodeNAnd struct {
	neg Node
	reg Node

	negID LID
	regID LID
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

func (n *nodeNAnd) Next() LID {
	for !n.regID.IsNull() {
		for n.negID.Less(n.regID) {
			n.readNeg()
		}
		if n.negID.IsNull() || n.negID != n.regID {
			cur := n.regID
			n.readReg()
			return cur
		}
		n.readReg()
	}
	return NullLID()
}

func (n *nodeNAnd) NextGeq(nextID LID) LID {
	lid := n.Next()
	for lid.Less(nextID) {
		lid = n.Next()
	}
	return lid
}

type nodeNAndBatched struct {
	reg  BatchedNode
	neg  BatchedNode
	desc bool

	regBatch LIDBatch
	negBatch LIDBatch
	negDone  bool
}

func NewNAndBatched(neg, reg BatchedNode, desc bool) BatchedNode {
	return &nodeNAndBatched{
		reg:      reg,
		neg:      neg,
		desc:     desc,
		negDone:  false,
		regBatch: EmptyBatch(),
		negBatch: EmptyBatch(),
	}
}

func (n *nodeNAndBatched) String() string {
	return fmt.Sprintf("(%s NAND %s)", n.neg.String(), n.reg.String())
}

func (n *nodeNAndBatched) NextBatch(need int) LIDBatch {
	if n.desc {
		return n.NextBatchGeq(need, NewDescZeroLID())
	}
	return n.NextBatchGeq(need, NewAscZeroLID())
}

func (n *nodeNAndBatched) NextBatchGeq(need int, minLID LID) LIDBatch {
	for {
		if n.regBatch.IsEmpty() {
			n.regBatch = n.reg.NextBatchGeq(need, minLID)
			if n.regBatch.IsEmpty() {
				return EmptyBatch()
			}
		}
		if !n.negDone && n.negBatch.IsEmpty() {
			n.negBatch = n.neg.NextBatchGeq(need, minLID)
			if n.negBatch.IsEmpty() {
				n.negDone = true
			}
		}

		result, regResidual, negResidual := AndNot(n.regBatch, n.negBatch, n.desc)
		n.regBatch = regResidual
		n.negBatch = negResidual

		if !result.IsEmpty() {
			return result
		}
	}
}
