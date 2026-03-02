package node

type sourcedNodeWrapper struct {
	node   Node
	source uint32
}

func (*sourcedNodeWrapper) String() string {
	return "SOURCED"
}

func (w *sourcedNodeWrapper) NextSourced() (LID, uint32) {
	cmp := w.node.Next()
	return cmp, w.source
}

func (w *sourcedNodeWrapper) NextSourcedGeq(nextID CmpLID) (CmpLID, uint32) {
	id := w.node.NextGeq(nextID)
	return id, w.source
}

func NewSourcedNodeWrapper(d Node, source int) Sourced {
	return &sourcedNodeWrapper{node: d, source: uint32(source)}
}

func WrapWithSource(nodes []Node) []Sourced {
	sourced := make([]Sourced, len(nodes))
	for i, n := range nodes {
		sourced[i] = NewSourcedNodeWrapper(n, i)
	}
	return sourced
}
