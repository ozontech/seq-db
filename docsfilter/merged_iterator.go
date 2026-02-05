package docsfilter

import "github.com/ozontech/seq-db/node"

func NewNMergedIterators(iterators []node.Node, reverse bool) node.Node {
	if len(iterators) == 0 {
		return &EmptyIterator{}
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	merged := NewMergedIterator(iterators[0], iterators[1], reverse)
	for _, s := range iterators[2:] {
		merged = NewMergedIterator(merged, s, reverse)
	}
	return merged
}

type MergedIterator struct {
	a, b       node.Node
	curA, curB uint32
	init       bool
	lessFn     func(a, b uint32) bool
}

func NewMergedIterator(
	a, b node.Node,
	reverse bool,
) node.Node {
	lessFn := func(a, b uint32) bool { return a < b }
	if reverse {
		lessFn = func(a, b uint32) bool { return a > b }
	}

	return &MergedIterator{
		a:      a,
		b:      b,
		init:   false,
		lessFn: lessFn,
	}
}

func (it *MergedIterator) String() string {
	return "MERGED_TOMBSTONES_ITERATOR"
}

func (it *MergedIterator) Next() (uint32, bool) {
	if !it.init {
		it.readA()
		it.readB()
		it.init = true
	}

	if it.a == nil && it.b == nil {
		return 0, false
	}
	if it.a == nil {
		return it.readB(), true
	}
	if it.b == nil {
		return it.readA(), true
	}

	if it.curA == it.curB {
		it.readA() // skip duplicate
		if it.a == nil {
			return it.readB(), true
		}
	}
	if it.lessFn(it.curB, it.curA) {
		return it.readB(), true
	}
	return it.readA(), true
}

func (it *MergedIterator) readA() uint32 {
	var has bool
	current := it.curA

	if it.curA, has = it.a.Next(); !has {
		it.a = nil // stop reading a
	}

	return current
}

func (it *MergedIterator) readB() uint32 {
	var has bool
	current := it.curB

	if it.curB, has = it.b.Next(); !has {
		it.b = nil // stop reading b
	}

	return current
}

type EmptyIterator struct{}

func (it *EmptyIterator) String() string {
	return "EMPTY_TOMBSTONES_ITERATOR"
}

func (it *EmptyIterator) Next() (uint32, bool) {
	return 0, false
}
