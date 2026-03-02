package node

type nodeRange struct {
	maxID LID
	curID LID
}

func (n *nodeRange) String() string {
	return "(RANGE)"
}

func NewRange(minVal, maxVal LID) *nodeRange {
	return &nodeRange{
		curID: minVal,
		maxID: maxVal,
	}
}

func (n *nodeRange) Next() LID {
	if n.maxID.Less(n.curID) {
		return NullCmpLID()
	}
	result := n.curID
	n.curID = n.curID.Inc()
	return result
}
