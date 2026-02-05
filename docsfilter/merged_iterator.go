package docsfilter

type TombstonesIterator interface {
	Next() (uint32, bool)
}

func NewNMergedIterators(iterators []TombstonesIterator) TombstonesIterator {
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
	a, b       TombstonesIterator
	curA, curB uint32
	init       bool
}

func NewMergedIterator(
	a, b TombstonesIterator,
) TombstonesIterator {
	return &MergedIterator{
		a:    a,
		b:    b,
		init: false,
	}
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
	if it.curB < it.curA {
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

func (it *EmptyIterator) Next() (uint32, bool) {
	return 0, false
}
