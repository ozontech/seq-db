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
		less:   GetLessFn(reverse),
		cur:    int(minVal),
		maxVal: maxVal,
		step:   step,
	}
}

func (n *nodeRange) Next(limit uint32) []uint32 {
	// TODO support batching
	if n.less(n.maxVal, uint32(n.cur)) {
		return nil
	}
	cur := uint32(n.cur)
	n.cur += n.step
	return []uint32{cur}
}
