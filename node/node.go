package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() CmpLID
	// NextGeq returns next greater or equal (GEQ) lid. Currently, some nodes do not support it
	// so the caller must check the output and be ready call it again if needed, like when using Next.
	// Therefore, nextID is more like a hint.
	NextGeq(nextID CmpLID) CmpLID
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id CmpLID, source uint32)
	NextSourcedGeq(nextLID CmpLID) (id CmpLID, source uint32)
}
