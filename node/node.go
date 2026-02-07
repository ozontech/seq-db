package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	// Next returns the next LID. Second return is false when exhausted.
	Next() (id uint32, has bool)
	// NextGeq returns the next LID >= minLID. Second return is false when exhausted.
	NextGeq(minLID uint32) (id uint32, has bool)
}

// BatchedNode extends Node with batch iteration. Only nodeAndBatched and LID iterators (IteratorAsc, IteratorDesc) implement it.
type BatchedNode interface {
	Node
	// NextBatch returns up to limit LIDs >= minLID. Returns nil when exhausted.
	NextBatch(minLID uint32, limit uint32) []uint32
}

type Sourced interface {
	fmt.Stringer // for testing
	NextSourced() (id uint32, source uint32, has bool)
	NextSourcedGeq(nextLID uint32) (id uint32, source uint32, has bool)
}
