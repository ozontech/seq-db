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

func (n *staticAsc) Next(limit uint32) []uint32 {
	if n.ptr >= len(n.data) {
		return nil
	}
	cur := n.data[n.ptr]
	n.ptr++
	return []uint32{cur}
}

func (n *staticDesc) Next(limit uint32) []uint32 {
	if n.ptr < 0 {
		return nil
	}
	cur := n.data[n.ptr]
	n.ptr--
	return []uint32{cur}
}

func (n *staticAsc) NextGeq(minLID uint32, limit uint32) []uint32 {
	return n.Next(limit)
}

func (n *staticDesc) NextGeq(minLID uint32, limit uint32) []uint32 {
	return n.Next(limit)
}

// MakeStaticNodes is currently used only for tests
func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, false)
	}
	return nodes
}
