package exec

import (
	"slices"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query"
)

// TODO: use existing type ???
type Order byte

const (
	OrderAsc Order = iota
	OrderDesc
)

// TODO: ???
type ExecutorState byte

const (
	ExecutorStateReadingInput ExecutorState = iota
	ExecutorStateProducingOutput
	ExecutorStateDone
)

type DocSorter struct {
	state ExecutorState
	input query.RecordProducer

	colIdx int
	field  string
	less   func(string, string) bool

	sortingBuf []*query.Record

	curIdx int
}

func NewDocSorter(
	input query.RecordProducer,
	colIdx int,
	field string,
	order Order,
) *DocSorter {
	less := func(a, b string) bool {
		return a < b
	}
	if order == OrderDesc {
		less = func(a, b string) bool {
			return a > b
		}
	}

	return &DocSorter{
		input:  input,
		colIdx: colIdx,
		field:  field,
		less:   less,
	}
}

func (s *DocSorter) Next() (*query.Record, *query.Metadata) {
	for s.state == ExecutorStateReadingInput {
		r, meta := s.input.Next()
		if meta != nil {
			return nil, meta
		}
		if r == nil {
			s.state = ExecutorStateProducingOutput
			break
		}

		s.sortingBuf = append(s.sortingBuf, r)
	}

	slices.SortFunc(s.sortingBuf, func(a, b *query.Record) int {
		if a == nil || b == nil {
			return 0 // TODO: ???
		}

		aVal := a.Vals[s.colIdx].Decoded().(*insaneJSON.Root).Dig(s.field).AsString()
		bVal := b.Vals[s.colIdx].Decoded().(*insaneJSON.Root).Dig(s.field).AsString()

		if aVal == bVal {
			return 0
		}
		if s.less(aVal, bVal) {
			return -1
		}
		return 1
	})

	if s.curIdx >= len(s.sortingBuf) {
		s.state = ExecutorStateDone
		return nil, nil
	}

	r := s.sortingBuf[s.curIdx]
	s.curIdx++

	return r, nil
}
