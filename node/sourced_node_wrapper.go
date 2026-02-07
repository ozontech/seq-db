package node

type sourcedNodeWrapper struct {
	node   Node
	source uint32
	batch  []uint32
	idx    int
}

func (*sourcedNodeWrapper) String() string {
	return "SOURCED"
}

func (w *sourcedNodeWrapper) NextSourced() (uint32, uint32, bool) {
	// If current batch is exhausted, get next batch
	for w.idx >= len(w.batch) {
		// TODO support batching
		w.batch = w.node.Next(1)
		w.idx = 0
		if w.batch == nil {
			return 0, w.source, false
		}
	}

	id := w.batch[w.idx]
	w.idx++
	return id, w.source, true
}

func (w *sourcedNodeWrapper) NextSourcedGeq(minLID uint32) (uint32, uint32, bool) {
	id, has := w.node.NextGeq(minLID)
	return id, w.source, has
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
