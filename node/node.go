package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() LID
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id LID, source uint32)
}
