package filtermanager

import "github.com/ozontech/seq-db/node"

func NewNMergedIterators(iterators []node.Node) node.Node {
	if len(iterators) == 0 {
		return &EmptyIterator{}
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	merged := NewMergedIterator(iterators[0], iterators[1])
	for _, s := range iterators[2:] {
		merged = NewMergedIterator(merged, s)
	}
	return merged
}

type MergedIterator struct {
	a, b       node.Node
	curA, curB node.LID
	init       bool
}

func NewMergedIterator(a, b node.Node) node.Node {
	return &MergedIterator{
		a:    a,
		b:    b,
		init: false,
	}
}

func (it *MergedIterator) String() string {
	return "MERGED_TOMBSTONES_ITERATOR"
}

func (it *MergedIterator) Next() node.LID {
	if !it.init {
		it.readA()
		it.readB()
		it.init = true
	}

	if it.a == nil && it.b == nil {
		return node.NullLID()
	}
	if it.a == nil {
		return it.readB()
	}
	if it.b == nil {
		return it.readA()
	}

	if it.curA.Eq(it.curB) {
		it.readA() // skip duplicate
		if it.a == nil {
			return it.readB()
		}
	}
	if it.curB.Less(it.curA) {
		return it.readB()
	}
	return it.readA()
}

func (it *MergedIterator) readA() node.LID {
	current := it.curA

	if it.curA = it.a.Next(); it.curA.IsNull() {
		it.a = nil // stop reading a
	}

	return current
}

func (it *MergedIterator) readB() node.LID {
	current := it.curB

	if it.curB = it.b.Next(); it.curB.IsNull() {
		it.b = nil // stop reading b
	}

	return current
}

type EmptyIterator struct{}

func (it *EmptyIterator) String() string {
	return "EMPTY_TOMBSTONES_ITERATOR"
}

func (it *EmptyIterator) Next() node.LID {
	return node.NullLID()
}
