package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() LID
	// NextGeq returns next greater or equal (GEQ) lid
	NextGeq(nextID LID) LID
}

type BatchedNode interface {
	fmt.Stringer
	// NextBatch returns next batch. Returns nil when exhausted.
	NextBatch() LIDBatch
	// NextBatchGeq returns next batch (LIDs >= minLID). Returns nil when exhausted.
	NextBatchGeq(nextLID LID) LIDBatch
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id LID, source uint32)
	NextSourcedGeq(nextLID LID) (id LID, source uint32)
}
