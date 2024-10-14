package node

var (
	emptyNode        = NewStatic(nil, false)
	emptyNodeSourced = NewSourcedNodeWrapper(emptyNode, 0)
)

func BuildORTree(nodes []Node, reverse bool) Node {
	return TreeFold(
		func(l, r Node) Node { return NewOr(l, r, reverse) },
		emptyNode,
		nodes,
	)
}

func BuildORTreeAgg(nodes []Node) Sourced {
	return TreeFold(NewNodeOrAgg,
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
	return op(treeFold(op, values[:mid]), treeFold(op, values[mid:]))
}
