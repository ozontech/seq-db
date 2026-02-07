package node

type staticNode struct {
	data     []uint32
	returned bool
}

func (n *staticNode) String() string {
	return "STATIC"
}

func NewStatic(data []uint32, reverse bool) Node {
	_ = reverse
	return &staticNode{
		data:     data,
		returned: false,
	}
}

func (n *staticNode) Next(limit uint32) []uint32 {
	if n.returned {
		return nil
	}
	n.returned = true
	return n.data
}

func (n *staticAsc) NextGeq(minLID uint32) []uint32 {
	return n.Next()
}

func (n *staticDesc) NextGeq(minLID uint32) []uint32 {
	return n.Next()
}

// MakeStaticNodes is currently used only for tests
func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, false)
	}
	return nodes
}
