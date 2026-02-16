package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() CmpLID
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id CmpLID, source uint32)
}
