package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() (id uint32, has bool)
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id uint32, source uint32, has bool)
}
