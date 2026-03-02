package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() LID
	// NextGeq returns next greater or equal (GEQ) lid. Currently, some nodes do not support it
	// so the caller must check the output and be ready call it again if needed, like when using Next.
	// Therefore, nextID is more like a hint.
	NextGeq(nextID LID) LID
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id LID, source uint32)
	NextSourcedGeq(nextLID LID) (id LID, source uint32)
}
