package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() (id uint32, has bool)
	// NextGeq returns next greater or equal (GEQ) lid. Currently, some nodes do not support it
	// so the caller must check the output and be ready call it again if needed.
	NextGeq(minLID uint32) (id uint32, has bool)
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id uint32, source uint32, has bool)
	NextSourcedGeq(nextLID uint32) (id uint32, source uint32, has bool)
}
