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

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id LID, source uint32)
	NextSourcedGeq(nextLID LID) (id LID, source uint32)
}
