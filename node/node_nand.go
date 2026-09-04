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
	reg BatchedNode
	neg BatchedNode
	asc bool

	regBatch LIDBatch
	negBatch LIDBatch
	negDone  bool
}

// NewNAndBatched returns a BatchedNode that computes AND NOT of two batched iterators.
// asc is the LID traversal order for NextBatch / NextBatchGeq (true = low to high).
func NewNAndBatched(neg, reg BatchedNode, asc bool) BatchedNode {
	return &nodeNAndBatched{
		reg:      reg,
		neg:      neg,
		asc:      asc,
		negDone:  false,
		regBatch: EmptyBatch(),
		negBatch: EmptyBatch(),
	}
}

func (n *nodeNAndBatched) String() string {
	return fmt.Sprintf("(%s NAND %s)", n.neg.String(), n.reg.String())
}

func (n *nodeNAndBatched) NextBatch() LIDBatch {
	if n.asc {
		return n.NextBatchGeq(NewAscZeroLID())
	}
	return n.NextBatchGeq(NewDescZeroLID())
}

func (n *nodeNAndBatched) NextBatchGeq(nextID LID) LIDBatch {
	for {
		if n.regBatch.IsEmpty() {
			n.regBatch = n.reg.NextBatchGeq(nextID)
			if n.regBatch.IsEmpty() {
				return EmptyBatch()
			}
		}
		if !n.negDone && n.negBatch.IsEmpty() {
			n.negBatch = n.neg.NextBatchGeq(nextID)
			if n.negBatch.IsEmpty() {
				n.negDone = true
			}
		}

		result, regResidual, negResidual := AndNot(n.regBatch, n.negBatch, n.asc)
		n.regBatch = regResidual
		n.negBatch = negResidual

		if !result.IsEmpty() {
			return result
		}
	}
}
