package node

type nodeRange struct {
	maxID CmpLID
	curID CmpLID
}

func (n *nodeRange) String() string {
	return "(RANGE)"
}

func NewRange(minVal, maxVal CmpLID) *nodeRange {
	return &nodeRange{
		curID: minVal,
		maxID: maxVal,
	}
}

func (n *nodeRange) Next() CmpLID {
	if n.maxID.Less(n.curID) {
		return NullCmpLID()
	}
	result := n.curID
	n.curID = n.curID.Inc()
	return result
}
