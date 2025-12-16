package active2

import "github.com/ozontech/seq-db/seq"

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

type IDIteratorItem struct {
	i   int
	id  seq.ID
	pos seq.DocPos
}

type IDIterator struct {
	i      int
	offset int
	idx    *memIndex
	posMap []seq.DocPos
}

func (it *IDIterator) Next() (v IDIteratorItem, has bool) {
	if it.offset < len(it.idx.ids) {
		v.i = it.i
		v.id = it.idx.ids[it.offset]
		v.pos = it.posMap[it.offset]
		has = true
		it.offset++
	}
	return v, has
}

type TokenIteratorPayload struct {
	idx     *memIndex
	lidsMap []uint32
}

type TokenIteratorItem struct {
	tid     uint32
	fid     uint32
	payload *TokenIteratorPayload
}

func (i *TokenIteratorItem) Field() []byte {
	return i.payload.idx.fields[i.fid]
}

func (i *TokenIteratorItem) Token() []byte {
	return i.payload.idx.tokens[i.tid]
}

func (i *TokenIteratorItem) LIDs() []uint32 {
	return i.payload.idx.tokenLIDs[i.tid]
}

func (i *TokenIteratorItem) lidsMap() []uint32 {
	return i.payload.lidsMap
}

type TokenIterator struct {
	tid          uint32
	fid          uint32
	fieldLastTID uint32
	payload      TokenIteratorPayload
}

func NewTokenIterator(idx *memIndex, lidsMap []uint32) *TokenIterator {
	return &TokenIterator{
		fieldLastTID: idx.fieldsTokens[string(idx.fields[0])].count - 1,
		payload: TokenIteratorPayload{
			idx:     idx,
			lidsMap: lidsMap,
		},
	}
}

func (it *TokenIterator) Next() (v TokenIteratorItem, has bool) {
	if int(it.tid) < len(it.payload.idx.tokens) {
		v.tid = uint32(it.tid)
		v.fid = uint32(it.fid)
		v.payload = &it.payload
		has = true
		it.tid++

		if it.tid > it.fieldLastTID {
			it.fid++
			if int(it.fid) < len(it.payload.idx.fields) {
				it.fieldLastTID += it.payload.idx.fieldsTokens[string(it.payload.idx.fields[it.fid])].count
			}
		}
	}
	return v, has
}
