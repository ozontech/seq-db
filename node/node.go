package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	Next() []uint32
	// NextGeq returns next greater or equal (GEQ) lid. Currently, some nodes do not support it
	// so the caller must check the output and be ready call it again if needed.
	NextGeq(minLID uint32) []uint32
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id uint32, source uint32, has bool)
	NextSourcedGeq(nextLID uint32) (id uint32, source uint32, has bool)
}

// singleIter wraps a batch-returning Node to yield single elements.
type singleIter struct {
	node  Node
	batch []uint32
}

func (s *singleIter) next() (uint32, bool) {
	for len(s.batch) == 0 {
		// TODO ?
		s.batch = s.node.Next(1)
		if s.batch == nil {
			return 0, false
		}
	}
	id := s.batch[0]
	s.batch = s.batch[1:]
	return id, true
}
