package node

import (
	"fmt"
)

type Node interface {
	fmt.Stringer // for testing
	// Next returns a batch of IDs. Returns nil when exhausted.
	Next(limit uint32) []uint32
}

type Sourced interface {
	fmt.Stringer // for testing
	// aggregation need source
	NextSourced() (id uint32, source uint32, has bool)
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
