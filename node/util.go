package node

import (
	"fmt"
	"slices"
)

const maxBatchDrain = 4 * 1024

// batcherNode allows to iterate over non-batched iterator batch by batch.
// A caller must immediately consume a yielded batch after calling NextBatch, since
// the underlying slice is reused.
type batcherNode struct {
	source Node
	desc   bool
	batch  []uint32
}

func NewBatcherNode(source Node, desc bool) BatchedNode {
	return &batcherNode{
		source: source,
		desc:   desc,
		batch:  make([]uint32, 0, maxBatchDrain),
	}
}

func (b *batcherNode) NextBatch() LIDBatch {
	b.batch = b.batch[:0]
	for len(b.batch) < maxBatchDrain {
		lid := b.source.Next()
		if lid.IsNull() {
			break
		}
		b.batch = append(b.batch, lid.Unpack())
	}
	if !b.desc {
		slices.Reverse(b.batch)
	}
	return NewSliceBatch(b.batch)
}

func (b *batcherNode) NextBatchGeq(nextID LID) LIDBatch {
	b.batch = b.batch[:0]
	for len(b.batch) < maxBatchDrain {
		lid := b.source.NextGeq(nextID)
		if lid.IsNull() {
			break
		}
		b.batch = append(b.batch, lid.Unpack())
	}
	if !b.desc {
		slices.Reverse(b.batch)
	}
	return NewSliceBatch(b.batch)
}

func (b *batcherNode) String() string {
	return fmt.Sprintf("(BATCH %s)", b.source.String())
}

type batchedEmpty struct{}

func EmptyBatched() BatchedNode {
	return &batchedEmpty{}
}

func (e *batchedEmpty) String() string {
	return "EMPTY_BATCHED"
}

func (e *batchedEmpty) NextBatch() LIDBatch {
	return EmptyBatch()
}

func (e *batchedEmpty) NextBatchGeq(_ LID) LIDBatch {
	return EmptyBatch()
}
