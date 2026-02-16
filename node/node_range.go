package node

type nodeRange struct {
	reverse bool

	maxVal uint32
	cur    int
	step   int
}

func (n *nodeRange) String() string {
	return "(RANGE)"
}

func NewRange(minVal, maxVal uint32, reverse bool) *nodeRange {
	step := 1
	if reverse {
		step = -1
		minVal, maxVal = maxVal, minVal
	}
	return &nodeRange{
		reverse: reverse,
		cur:     int(minVal),
		maxVal:  maxVal,
		step:    step,
	}
}

func (n *nodeRange) Next() CmpLID {
	curCmp := NewCmpLID(uint32(n.cur), n.reverse)
	maxCmp := NewCmpLID(n.maxVal, n.reverse)
	if maxCmp.Less(curCmp) {
		return NullCmpLID()
	}
	cur := uint32(n.cur)
	n.cur += n.step
	return NewCmpLID(cur, n.reverse)
}
