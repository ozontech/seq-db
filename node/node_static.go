package node

import (
	"math"
	"sort"
)

type staticCursor struct {
	ptr  int
	data []uint32
}

// staticAsc stores lids in data slice in ascending order, and iterates in increasing order
type staticAsc struct {
	staticCursor
}

// staticDesc stores lids in data slice in ascending order, but iterates from the end (in descending order)
type staticDesc struct {
	staticCursor
}

func (n *staticCursor) String() string {
	return "STATIC"
}

// NewStatic returns a Node over sorted LID data.
// asc=true iterates low to high; asc=false iterates high to low.
func NewStatic(data []uint32, asc bool) Node {
	if !asc {
		return &staticDesc{staticCursor: staticCursor{
			ptr:  len(data) - 1,
			data: data,
		}}
	}

	return &staticAsc{staticCursor: staticCursor{
		ptr:  0,
		data: data,
	}}
}

func (n *staticAsc) Next() LID {
	// LID ascending: return AscLID
	if n.ptr >= len(n.data) {
		return NewAscLID(math.MaxUint32)
	}
	cur := n.data[n.ptr]
	n.ptr++
	return NewAscLID(cur)
}

// NextGeq finds next greater or equals since iteration is in ascending order
func (n *staticAsc) NextGeq(nextID LID) LID {
	if n.ptr >= len(n.data) {
		return NullLID()
	}

	from := n.ptr
	idx := sort.Search(len(n.data)-from, func(i int) bool { return n.data[from+i] >= nextID.Unpack() })
	if idx >= len(n.data)-from {
		return NullLID()
	}

	i := from + idx
	cur := n.data[i]
	n.ptr = i + 1
	return NewAscLID(cur)
}

func (n *staticDesc) Next() LID {
	// LID descending: return DescLID
	if n.ptr < 0 {
		return NewDescLID(0)
	}
	cur := n.data[n.ptr]
	n.ptr--
	return NewDescLID(cur)
}

// NextGeq finds next less or equals since iteration is in descending order
func (n *staticDesc) NextGeq(nextID LID) LID {
	if n.ptr < 0 {
		return NullLID()
	}
	idx := sort.Search(n.ptr+1, func(i int) bool { return n.data[i] > nextID.Unpack() }) - 1
	if idx < 0 {
		return NullLID()
	}

	cur := n.data[idx]
	n.ptr = idx - 1
	return NewDescLID(cur)
}

// MakeStaticNodes is currently used only for tests
func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, true)
	}
	return nodes
}

type staticBatchedAsc struct {
	staticCursor
	batch LIDBatch
}

type staticBatchedDesc struct {
	staticCursor
	batch LIDBatch
}

// NewStaticBatched returns a BatchedNode over sorted LID data.
// asc=true iterates low to high; asc=false iterates high to low.
func NewStaticBatched(data []uint32, asc bool) BatchedNode {
	if !asc {
		return &staticBatchedDesc{staticCursor: staticCursor{
			ptr:  len(data) - 1,
			data: data,
		}, batch: EmptyBatch()}
	}

	return &staticBatchedAsc{staticCursor: staticCursor{
		ptr:  0,
		data: data,
	}, batch: EmptyBatch()}
}

func (n *staticBatchedAsc) String() string {
	return "STATIC_BATCHED_ASC"
}

func (n *staticBatchedAsc) NextBatch() LIDBatch {
	return n.NextBatchGeq(NewAscZeroLID())
}

func (n *staticBatchedAsc) NextBatchGeq(nextID LID) LIDBatch {
	for {
		if n.batch.IsEmpty() {
			if n.ptr >= len(n.data) {
				return EmptyBatch()
			}
			n.batch = NewSliceBatch(n.data[n.ptr:])
			n.ptr = len(n.data)
		}

		if n.batch.IsEmpty() {
			continue
		}
		if nextID.Unpack() > n.batch.Max() {
			n.batch = EmptyBatch()
			continue
		}

		out := n.batch.Narrow(nextID.Unpack(), math.MaxUint32)
		n.batch = EmptyBatch()
		return out
	}
}

func (n *staticBatchedDesc) String() string {
	return "STATIC_BATCHED_DESC"
}

func (n *staticBatchedDesc) NextBatch() LIDBatch {
	return n.NextBatchGeq(NewDescZeroLID())
}

func (n *staticBatchedDesc) NextBatchGeq(nextID LID) LIDBatch {
	for {
		if n.batch.IsEmpty() {
			if n.ptr < 0 {
				return EmptyBatch()
			}
			n.batch = NewSliceBatch(n.data[:n.ptr+1])
			n.ptr = -1
		}

		if n.batch.IsEmpty() {
			continue
		}
		if nextID.Unpack() < n.batch.Min() {
			n.batch = EmptyBatch()
			continue
		}

		out := n.batch.Narrow(0, nextID.Unpack())
		n.batch = EmptyBatch()
		return out
	}
}
