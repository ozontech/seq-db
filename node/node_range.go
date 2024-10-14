package node

type nodeRange struct {
	less LessFn

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
		less: GetLessFn(reverse),

		cur:    int(minVal),
		maxVal: maxVal,
		step:   step,
	}
}

func (n *nodeRange) Next() (uint32, bool) {
	if n.less(n.maxVal, uint32(n.cur)) {
		return 0, false
	}
	cur := uint32(n.cur)
	n.cur += n.step
	return cur, true
}
