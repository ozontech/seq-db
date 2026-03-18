package node

import "math"

type staticCursor struct {
	ptr  int
	data []uint32
}

type staticAsc struct {
	staticCursor
}

type staticDesc struct {
	staticCursor
}

func (n *staticCursor) String() string {
	return "STATIC"
}

func NewStatic(data []uint32, reverse bool) Node {
	if reverse {
		return &staticDesc{staticCursor: staticCursor{
			ptr:  len(data) - 1,
			data: data,
		}}
	}

	return &staticAsc{staticCursor: staticCursor{
		ptr:  0,
		data: data,
	}}
}

func (n *staticAsc) Next() LID {
	// staticAsc is used in docs order desc, hence we return LID with desc order
	if n.ptr >= len(n.data) {
		return NewLIDDesc(math.MaxUint32)
	}
	cur := n.data[n.ptr]
	n.ptr++
	return NewLIDDesc(cur)
}

func (n *staticDesc) Next() LID {
	// staticDesc is used in docs order asc, hence we return LID with asc order
	if n.ptr < 0 {
		return NewLIDAsc(0)
	}
	cur := n.data[n.ptr]
	n.ptr--
	return NewLIDAsc(cur)
}

// MakeStaticNodes  is currently used only for tests
func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, false)
	}
	return nodes
}
