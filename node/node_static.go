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

func (n *staticAsc) Init() {

}

func (n *staticAsc) Next() (uint32, bool) {
	if n.ptr >= len(n.data) {
		return 0, false
	}
	cur := n.data[n.ptr]
	n.ptr++
	return cur, true
}

func (n *staticDesc) Init() {

}

func (n *staticDesc) Next() (uint32, bool) {
	if n.ptr < 0 {
		return 0, false
	}
	cur := n.data[n.ptr]
	n.ptr--
	return cur, true
}

func (n *staticAsc) NextGeq(minLID uint32) (uint32, bool) {
	// advance ptr to first >= minLID
	for n.ptr < len(n.data) && n.data[n.ptr] < minLID {
		n.ptr++
	}
	return n.Next()
}

func (n *staticDesc) NextGeq(minLID uint32) (uint32, bool) {
	for n.ptr >= 0 && n.data[n.ptr] < minLID {
		n.ptr--
	}
	return n.Next()
}

func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, false)
	}
	return nodes
}
