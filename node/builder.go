package node

var (
	emptyNode        = NewStatic(nil, true)
	emptyNodeSourced = NewSourcedNodeWrapper(emptyNode, 0)
)

func BuildORTree(nodes []Node) Node {
	return TreeFold(
		func(l, r Node) Node { return NewOr(l, r) },
		emptyNode,
		nodes,
	)
}

func BuildORTreeAgg(nodes []Node) Sourced {
	return TreeFold(
		NewNodeOrAgg,
		emptyNodeSourced,
		WrapWithSource(nodes),
	)
}

func TreeFold[V any](op func(V, V) V, def V, values []V) V {
	if len(values) == 0 {
		return def
	}

	return treeFold(op, values)
}

func treeFold[V any](op func(V, V) V, values []V) V {
	if len(values) == 1 {
		return values[0]
	}

	mid := len(values) / 2

	// single call site to prevent stack traces explosion of alloc samples on ultra-deep OR trees
	var children [2]V
	for i, part := range [2][]V{values[:mid], values[mid:]} {
		children[i] = treeFold(op, part)
	}
	return op(children[0], children[1])
}
