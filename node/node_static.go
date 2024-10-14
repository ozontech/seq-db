package node

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

func (n *staticAsc) Next() (uint32, bool) {
	if n.ptr >= len(n.data) {
		return 0, false
	}
	cur := n.data[n.ptr]
	n.ptr++
	return cur, true
}

func (n *staticDesc) Next() (uint32, bool) {
	if n.ptr < 0 {
		return 0, false
	}
	cur := n.data[n.ptr]
	n.ptr--
	return cur, true
}

// MakeStaticNodes  is currently used only for tests
func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, false)
	}
	return nodes
}
