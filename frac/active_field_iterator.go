package frac

import (
	"slices"

	"github.com/ozontech/seq-db/frac/processor"
)

type fieldIterator struct {
	lids [][]uint32
	pos  int
}

func NewFieldIterator(sources [][]uint32) processor.FieldLIDs {
	return &fieldIterator{lids: sources}
}

func (p *fieldIterator) NextBatch(lids, offsets []uint32) ([]uint32, []uint32, bool) {
	for p.pos < len(p.lids) && len(p.lids[p.pos]) == 0 {
		p.pos++
	}
	if p.pos >= len(p.lids) {
		return nil, nil, false
	}

	offsets = append(offsets, 0)

	for p.pos < len(p.lids) {
		cur := p.lids[p.pos]
		if len(cur) == 0 {
			p.pos++
			continue
		}

		needLids := len(lids) + len(cur)
		needLists := len(offsets)
		needOffsets := needLists + 1
		if len(offsets) > 1 &&
			(needLids > cap(lids) || needOffsets > cap(offsets)) {
			break
		}

		lids = append(slices.Grow(lids, len(cur)), cur...)
		offsets = append(offsets, uint32(len(lids)))
		p.pos++
	}
	return lids, offsets, true
}
