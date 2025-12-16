package active2

type IOrderedIterator[T any] interface {
	Next() (T, bool)
}

func MergeKSortIterators[T any](src []IOrderedIterator[T], cmp func(T, T) int) IOrderedIterator[T] {
	n := len(src)
	if n == 1 {
		return src[0]
	}
	h := n / 2
	src1 := MergeKSortIterators(src[:h], cmp)
	src2 := MergeKSortIterators(src[h:], cmp)
	return NewMergeIterator(src1, src2, cmp)
}

type MergeIterator[T any] struct {
	v1, v2     T
	has1, has2 bool
	src1, src2 IOrderedIterator[T]
	cmp        func(T, T) int
}

func NewMergeIterator[T any](src1, src2 IOrderedIterator[T], cmp func(T, T) int) *MergeIterator[T] {
	r := MergeIterator[T]{
		src1: src1,
		src2: src2,
		cmp:  cmp,
	}
	r.Init()
	return &r
}

func (s *MergeIterator[T]) Init() {
	s.v1, s.has1 = s.src1.Next()
	s.v2, s.has2 = s.src2.Next()
}

func (s *MergeIterator[T]) Next() (v T, has bool) {
	if s.has1 && s.has2 {
		if s.cmp(s.v1, s.v2) < 0 {
			v = s.v1
			s.v1, s.has1 = s.src1.Next()
		} else {
			v = s.v2
			s.v2, s.has2 = s.src2.Next()
		}
		return v, true
	}
	if s.has1 {
		v = s.v1
		s.v1, s.has1 = s.src1.Next()
		return v, true
	}
	if s.has2 {
		v = s.v2
		s.v2, s.has2 = s.src2.Next()
		return v, true
	}
	return v, false
}
