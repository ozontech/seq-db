package active2

import (
	"cmp"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type streamsPool[T Ordered] struct {
	ts  []TwoStreams[T]
	oss []OneSliceStream[T]
	tss []TwoSliceStream[T]
}

func (s *streamsPool[T]) GetTwoStreams(src1, src2 IOrderedStream[T]) (r *TwoStreams[T]) {
	s.ts = append(s.ts, TwoStreams[T]{})
	r = &s.ts[len(s.ts)-1]
	r.src1 = src1
	r.src2 = src2
	r.Init()

	return r
}

func (s *streamsPool[T]) GetTwoSliceStream(src1, src2 []T) (r *TwoSliceStream[T]) {
	s.tss = append(s.tss, TwoSliceStream[T]{})
	r = &s.tss[len(s.tss)-1]
	r.src1 = src1
	r.src2 = src2
	return r
}

func (s *streamsPool[T]) GetOneSliceStream(src []T) (r *OneSliceStream[T]) {
	s.oss = append(s.oss, OneSliceStream[T]{})
	r = &s.oss[len(s.oss)-1]
	r.src = src
	return r
}

func (s *streamsPool[T]) Reset() {
	s.ts = s.ts[:0]
	s.oss = s.oss[:0]
	s.tss = s.tss[:0]
}

func MergeSortNSlices[T Ordered](src [][]T, p *streamsPool[T]) IOrderedStream[T] {
	n := len(src)
	if n == 1 {
		return p.GetOneSliceStream(src[0])
	}
	if n == 2 {
		return p.GetTwoSliceStream(src[0], src[1])
	}
	h := n / 2
	src1 := MergeSortNSlices(src[:h], p)
	src2 := MergeSortNSlices(src[h:], p)
	return p.GetTwoStreams(src1, src2)
}

type IOrderedStream[T Ordered] interface {
	Next() (T, bool)
}

type TwoStreams[T Ordered] struct {
	src1, src2 IOrderedStream[T]
	v1, v2     T
	has1, has2 bool
}

func (s *TwoStreams[T]) Init() {
	s.v1, s.has1 = s.src1.Next()
	s.v2, s.has2 = s.src2.Next()
}

func (s *TwoStreams[T]) Next() (v T, has bool) {
	if s.has1 && s.has2 {
		if s.v1 < s.v2 {
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

type OneSliceStream[T cmp.Ordered] struct {
	p   int
	src []T
}

func (s *OneSliceStream[T]) Next() (v T, has bool) {
	if s.p < len(s.src) {
		has = true
		v = s.src[s.p]
		s.p++
	}
	return v, has
}

type TwoSliceStream[T Ordered] struct {
	p1, p2     int
	src1, src2 []T
}

func (s *TwoSliceStream[T]) Next() (v T, has bool) {
	n1, n2 := len(s.src1), len(s.src2)
	has1 := s.p1 < n1
	has2 := s.p2 < n2
	if has1 && has2 {

		v1 := s.src1[s.p1]
		v2 := s.src2[s.p2]
		if v1 < v2 {
			s.p1++
			return v1, true
		}
		s.p2++
		return v2, true
	}

	if has1 {
		v = s.src1[s.p1]
		s.p1++
		return v, true
	}

	if has2 {
		v = s.src2[s.p2]
		s.p2++
		return v, true
	}

	return v, false
}
