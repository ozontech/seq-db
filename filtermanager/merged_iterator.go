package filtermanager

import "github.com/ozontech/seq-db/node"

func NewNMergedIterators(iterators []node.Node) node.Node {
	if len(iterators) == 0 {
		return &EmptyIterator{}
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	merged := node.NewOr(iterators[0], iterators[1])
	for _, s := range iterators[2:] {
		merged = node.NewOr(merged, s)
	}

	return merged
}

type EmptyIterator struct{}

func (it *EmptyIterator) String() string {
	return "EMPTY_HIDE_FLAG_ITERATOR"
}

func (it *EmptyIterator) Next() node.LID {
	return node.NullLID()
}
