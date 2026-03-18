package node

import "github.com/ozontech/seq-db/util"
import (
	"math"
)

type staticCursor struct {
	ptr  int
	data []uint32
}

// staticAsc stores lids in data slice in ascending order, and iterates in increasing order
type staticAsc struct {
	staticCursor
}

// staticAsc stores lids in data slice in ascending order, but iterates from the end (in descending order)
type staticDesc struct {
	staticCursor
}

func (n *staticCursor) String() string {
	return "STATIC"
}

func NewStatic(data []uint32, reverse bool) Node {
	if reverse {
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
	// staticAsc is used in docs order desc, hence we return LID with desc order
	if n.ptr >= len(n.data) {
		return NewDescLID(math.MaxUint32)
	}
	cur := n.data[n.ptr]
	n.ptr++
	return NewDescLID(cur)
}

// NextGeq finds next greater or equals since iteration is in ascending order
func (n *staticAsc) NextGeq(nextID LID) LID {
	if n.ptr >= len(n.data) {
		return NewLIDOrderDesc(math.MaxUint32)
	}

	from := n.ptr
	idx, found := util.GallopSearchGeq(n.data[from:], nextID.Unpack())
	if !found {
		return NewLIDOrderDesc(math.MaxUint32)
	}

	i := from + idx
	cur := n.data[i]
	n.ptr = i + 1
	return NewLIDOrderDesc(cur)
}

func (n *staticDesc) Next() LID {
	// staticDesc is used in docs order asc, hence we return LID with asc order
	if n.ptr < 0 {
		return NewAscLID(0)
	}
	cur := n.data[n.ptr]
	n.ptr--
	return NewAscLID(cur)
}

// NextGeq finds next less or equals since iteration is in descending order
func (n *staticDesc) NextGeq(nextID LID) LID {
	if n.ptr < 0 {
		return NewLIDOrderAsc(0)
	}
	idx, found := util.GallopSearchLeq(n.data[:n.ptr+1], nextID.Unpack())
	if !found {
		return NewLIDOrderAsc(0)
	}

	cur := n.data[idx]
	n.ptr = idx - 1
	return NewLIDOrderAsc(cur)
}

// MakeStaticNodes is currently used only for tests
func MakeStaticNodes(data [][]uint32) []Node {
	nodes := make([]Node, len(data))
	for i, values := range data {
		nodes[i] = NewStatic(values, false)
	}
	return nodes
}
