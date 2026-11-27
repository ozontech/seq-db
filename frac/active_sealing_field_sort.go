package frac

import (
	"context"
	"encoding/binary"
	"iter"
	"slices"

	"github.com/bits-and-blooms/bitset"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
)

func (src *ActiveSealingSource) sortDocsByField(field string) error {
	var tids []uint32
	if index, ok := slices.BinarySearch(src.fields, field); ok {
		tids = src.fieldTIDs[index]
	}

	lidCount := len(src.sortedLIDs) + 1
	added := bitset.New(uint(lidCount))
	added.Set(0)
	duplicates := map[uint32]uint32{}

	var previous seq.ID
	var lastLIDForDuplicate uint32

	for i, lid := range src.sortedLIDs {
		newLID := uint32(i + 1)
		id := src.documentID(lid)

		if i > 0 && id == previous {
			duplicates[newLID] = lastLIDForDuplicate
			added.Set(uint(newLID))
		} else {
			lastLIDForDuplicate = newLID
		}

		previous = id
	}

	newOrder := make([]uint32, 1, lidCount-len(duplicates))
	for _, tid := range tids {
		for _, oldLID := range src.lids[tid].SortedLIDsUnsafe() {
			lid := src.oldToNewLIDs[oldLID]

			if duplOrigLID, ok := duplicates[lid]; ok {
				lid = duplOrigLID
			}

			if added.Test(uint(lid)) {
				continue
			}

			added.Set(uint(lid))
			newOrder = append(newOrder, lid)
		}
	}

	for lid := 1; lid < lidCount; lid++ {
		if added.Test(uint(lid)) {
			continue
		}
		newOrder = append(newOrder, uint32(lid))
	}

	if err := src.saveDocs(src.docsInOrder(newOrder), consts.StdocsTmpFileSuffix, consts.StdocsFileSuffix); err != nil {
		return err
	}

	positions := make([]seq.DocPos, lidCount)
	for i, lid := range newOrder {
		positions[lid] = src.docPosSorted[i]
	}
	for duplicate, duplOrigLID := range duplicates {
		positions[duplicate] = positions[duplOrigLID]
	}
	src.docPosSorted = positions

	return nil
}

func (src *ActiveSealingSource) documentID(lid uint32) seq.ID {
	return seq.ID{MID: seq.MID(src.mids.vals[lid]), RID: seq.RID(src.rids.vals[lid])}
}

func (src *ActiveSealingSource) docsInOrder(order []uint32) iter.Seq2[Document, error] {
	return func(yield func(Document, error) bool) {
		release, err := src.memoryLimiter.Acquire(context.Background(), src.info.DocsRaw)
		if err != nil {
			yield(Document{}, err)
			return
		}
		defer release()

		blocks := make([][]byte, len(src.blocksOffsets))
		for i := range blocks {
			block, _, err := src.docsReader.Load(uint32(i))
			if err != nil {
				yield(Document{}, err)
				return
			}
			blocks[i] = block
		}

		if !yield(Document{First: seq.SystemID}, nil) {
			return
		}
		for _, lid := range order[1:] {
			id := src.documentID(src.sortedLIDs[lid-1])
			blockIndex, offset := src.docPosMap[id].Unpack()
			block := blocks[blockIndex]
			docLen := binary.LittleEndian.Uint32(block[offset:])
			doc := block[offset+4 : offset+4+uint64(docLen)]
			if !yield(Document{First: id, Second: doc}, nil) {
				return
			}
		}
	}
}
