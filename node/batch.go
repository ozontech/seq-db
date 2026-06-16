package node

import (
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
)

// LIDBatch is batch of lids. It's immutable and can not be modified.
type LIDBatch interface {
	Len() int
	IsEmpty() bool
	// Min returns minimum (first) value. Panics if batch is empty.
	Min() uint32
	// Max returns max (last) value. Panics if batch is empty.
	Max() uint32
	CopyLIDs(desc bool, dst []LID) []LID
	// Iter iterates lids in ascending way.
	Iter() Iter
	// ReverseIter iterates lids in descending way.
	ReverseIter() Iter
	// Narrow returns a batch containing only LIDs from minLID to maxLID (inclusive both).
	Narrow(minLID, maxLID uint32) LIDBatch
}

type Iter interface {
	Next() (uint32, bool)
	NextGeq(geq uint32) (uint32, bool)
}

func NewBitmapBatch(b *roaring.Bitmap) LIDBatch {
	if b == nil || b.IsEmpty() {
		return EmptyBatch()
	}
	return &bitmapBatch{
		bm:  b,
		min: b.Minimum(),
		max: b.Maximum(),
	}
}

func NewBitmapBatchFromLids(lids []uint32) LIDBatch {
	if len(lids) == 0 {
		return EmptyBatch()
	}
	b := roaring.NewBitmap()
	b.AddMany(lids)
	b.RunOptimize()
	return NewBitmapBatch(b)
}

func NewSliceBatch(lids []uint32) LIDBatch {
	if len(lids) == 0 {
		return EmptyBatch()
	}
	return &sliceBatch{lids: lids}
}

// sliceBatch a batch of LIDs based on slice. LIDs are always sorted in ascending way.
// It's never empty.
type sliceBatch struct {
	lids []uint32
}

func (b *sliceBatch) Len() int {
	return len(b.lids)
}

func (b *sliceBatch) IsEmpty() bool {
	return false
}

func (b *sliceBatch) Min() uint32 {
	return b.lids[0]
}

func (b *sliceBatch) Max() uint32 {
	return b.lids[len(b.lids)-1]
}

func (b *sliceBatch) Narrow(minLID, maxLID uint32) LIDBatch {
	batchMin := b.lids[0]
	batchMax := b.lids[len(b.lids)-1]
	if minLID <= batchMin && batchMax <= maxLID {
		return b
	}
	if maxLID < batchMin || minLID > batchMax {
		return EmptyBatch()
	}
	lo := 0
	if minLID > batchMin {
		lo = sort.Search(len(b.lids), func(i int) bool { return b.lids[i] >= minLID })
	}
	hi := len(b.lids)
	if maxLID < batchMax {
		hi = sort.Search(len(b.lids), func(i int) bool { return b.lids[i] > maxLID })
	}
	if lo >= hi {
		return EmptyBatch()
	}
	return &sliceBatch{lids: b.lids[lo:hi]}
}

func (b *sliceBatch) Iter() Iter {
	return &sliceIter{lids: b.lids, max: b.Max()}
}

func (b *sliceBatch) ReverseIter() Iter {
	return &sliceReverseIter{lids: b.lids, idx: len(b.lids) - 1}
}

func (b *sliceBatch) CopyLIDs(desc bool, dst []LID) []LID {
	if desc {
		for _, lid := range b.lids {
			dst = append(dst, NewDescLID(lid))
		}
	} else {
		for i := len(b.lids) - 1; i >= 0; i-- {
			dst = append(dst, NewAscLID(b.lids[i]))
		}
	}
	return dst
}

type sliceIter struct {
	lids []uint32
	idx  int
	max  uint32
}

func (it *sliceIter) Next() (uint32, bool) {
	if it.idx >= len(it.lids) {
		return 0, false
	}
	v := it.lids[it.idx]
	it.idx++
	return v, true
}

func (it *sliceIter) NextGeq(geq uint32) (uint32, bool) {
	if it.idx >= len(it.lids) || (geq > it.max) {
		it.idx = len(it.lids)
		return 0, false
	}
	rest := it.lids[it.idx:]
	off := sort.Search(len(rest), func(i int) bool { return rest[i] >= geq })
	it.idx += off
	return it.Next()
}

type sliceReverseIter struct {
	lids []uint32
	idx  int
}

func (it *sliceReverseIter) Next() (uint32, bool) {
	if it.idx < 0 {
		return 0, false
	}
	v := it.lids[it.idx]
	it.idx--
	return v, true
}

func (it *sliceReverseIter) NextGeq(leq uint32) (uint32, bool) {
	if it.idx < 0 {
		return 0, false
	}
	right := it.idx + 1
	idx := sort.Search(right, func(i int) bool { return it.lids[i] > leq }) - 1
	if idx < 0 {
		it.idx = -1
		return 0, false
	}
	it.idx = idx
	return it.Next()
}

// bitmapBatch a LIDs batch based on roaring bitmap. Never empty.
type bitmapBatch struct {
	bm  *roaring.Bitmap
	min uint32
	max uint32
}

func (b *bitmapBatch) Len() int {
	return int(b.bm.GetCardinality())
}

func (b *bitmapBatch) IsEmpty() bool {
	return false
}

func (b *bitmapBatch) Min() uint32 {
	return b.min
}

func (b *bitmapBatch) Max() uint32 {
	return b.max
}

func (b *bitmapBatch) Narrow(minLID, maxLID uint32) LIDBatch {
	if minLID <= b.min && b.max <= maxLID {
		return b
	}
	if maxLID < b.min || minLID > b.max {
		return EmptyBatch()
	}
	// TODO(cheb0) use copy-on-write for bitmap?
	out := b.bm.Clone()
	if minLID > b.min {
		out.RemoveRange(0, uint64(minLID))
	}
	if maxLID < b.max {
		out.RemoveRange(uint64(maxLID)+1, uint64(0x100000000))
	}
	return NewBitmapBatch(out)
}

func (b *bitmapBatch) Iter() Iter {
	return newBitmapIter(b.bm)
}

func (b *bitmapBatch) ReverseIter() Iter {
	return newBitmapReverseIter(b.bm)
}

func (b *bitmapBatch) CopyLIDs(desc bool, dst []LID) []LID {
	if desc {
		it := b.bm.Iterator()
		for it.HasNext() {
			dst = append(dst, NewDescLID(it.Next()))
		}
	} else {
		it := b.bm.ReverseIterator()
		for it.HasNext() {
			dst = append(dst, NewAscLID(it.Next()))
		}
	}
	return dst
}

type emptyBatch struct{}

var emptyBatchInstance = emptyBatch{}

func EmptyBatch() LIDBatch {
	return emptyBatchInstance
}

func (emptyBatch) Len() int      { return 0 }
func (emptyBatch) IsEmpty() bool { return true }

func (emptyBatch) Min() uint32 {
	panic("min called on empty batch")
}

func (emptyBatch) Max() uint32 {
	panic("Maximum called on empty batch")
}

func (emptyBatch) Narrow(uint32, uint32) LIDBatch   { return emptyBatchInstance }
func (emptyBatch) CopyLIDs(_ bool, dst []LID) []LID { return dst }
func (emptyBatch) Iter() Iter                       { return emptyIterInstance }
func (emptyBatch) ReverseIter() Iter                { return emptyIterInstance }

type emptyIter struct{}

var emptyIterInstance = emptyIter{}

func (emptyIter) Next() (uint32, bool)          { return 0, false }
func (emptyIter) NextGeq(uint32) (uint32, bool) { return 0, false }

type bitmapIter struct {
	it roaring.IntIterator
}

func newBitmapIter(b *roaring.Bitmap) *bitmapIter {
	var it roaring.IntIterator
	it.Initialize(b)
	return &bitmapIter{it: it}
}

func (f *bitmapIter) Next() (uint32, bool) {
	if !f.it.HasNext() {
		return 0, false
	}
	return f.it.Next(), true
}

func (f *bitmapIter) NextGeq(geq uint32) (uint32, bool) {
	f.it.AdvanceIfNeeded(geq)
	if !f.it.HasNext() {
		return 0, false
	}
	return f.it.Next(), true
}

type bitmapReverseIter struct {
	bm  *roaring.Bitmap
	pos uint32
}

func newBitmapReverseIter(bm *roaring.Bitmap) *bitmapReverseIter {
	return &bitmapReverseIter{bm: bm, pos: math.MaxUint32}
}

func (it *bitmapReverseIter) Next() (uint32, bool) {
	prev := it.bm.PreviousValue(it.pos - 1)
	if prev == -1 {
		return 0, false
	}
	it.pos = uint32(prev)
	return uint32(prev), true
}

func (it *bitmapReverseIter) NextGeq(leq uint32) (uint32, bool) {
	prev := it.bm.PreviousValue(min(it.pos-1, leq))
	if prev == -1 {
		return 0, false
	}
	it.pos = uint32(prev)
	return uint32(prev), true
}
