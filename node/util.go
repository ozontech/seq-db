package node

import (
	"fmt"
	"slices"
)

const maxDrain = 4 * 1024

// batcherNode allows to iterate over non-batched iterator batch by batch
type batcherNode struct {
	source Node
	desc   bool
	batch  []uint32
}

func NewBatcherNode(source Node, desc bool) BatchedNode {
	return &batcherNode{
		source: source,
		desc:   desc,
		batch:  make([]uint32, 0, maxDrain),
	}
}

func (b *batcherNode) NextBatch(need int) LIDBatch {
	need = min(maxDrain, need)
	batch := b.batch[:0]
	polled := 0
	for polled < need {
		lid := b.source.Next()
		if lid.IsNull() {
			break
		}
		batch = append(batch, lid.Unpack())
		polled++
	}
	b.batch = batch[:0]
	if !b.desc {
		slices.Reverse(batch)
	}
	return NewSliceBatch(batch)
}

func (b *batcherNode) NextBatchGeq(need int, nextLID LID) LIDBatch {
	need = min(maxDrain, need)
	batch := b.batch[:0]
	polled := 0
	for polled < need {
		lid := b.source.NextGeq(nextLID)
		if lid.IsNull() {
			break
		}
		batch = append(batch, lid.Unpack())
		polled++
	}
	b.batch = batch[:0]
	if !b.desc {
		slices.Reverse(batch)
	}
	return NewSliceBatch(batch)
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

func (e *batchedEmpty) NextBatch(need int) LIDBatch {
	return EmptyBatch()
}

func (e *batchedEmpty) NextBatchGeq(need int, _ LID) LIDBatch {
	return EmptyBatch()
}
