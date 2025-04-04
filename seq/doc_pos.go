package seq

import (
	"math"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

const (
	docOffsetBits = 30 // this value (but not 32 bits) for historical reasons
	docOffsetMask = 1<<docOffsetBits - 1
	maxDocOffset  = docOffsetMask

	DocPosNotFound = DocPos(math.MaxUint64)
)

type DocPos uint64

func PackDocPos(blockIndex uint32, offset uint64) DocPos {
	if offset > maxDocOffset {
		logger.Panic("block offset too big",
			zap.Uint64("val", offset),
			zap.Int("max", maxDocOffset),
		)
	}

	pos := DocPos(blockIndex)<<docOffsetBits | DocPos(offset)

	// we add 1 to avoid DocPos = 0 (it was a "not found" sign)
	// this is not relevant now, but we leave it only for backward compatibility
	return pos + 1
}

func (pos DocPos) Unpack() (uint32, uint64) {
	pos-- // rollback "pos + 1" from PackDocPos func
	blockIndex := uint32(pos >> docOffsetBits)
	offset := uint64(pos & docOffsetMask)

	return blockIndex, offset
}

func GroupDocsOffsets(docsPos []DocPos) ([]uint32, [][]uint64, [][]int) {
	var (
		blocks  []uint32   // slice of all uniq blocks from source `docsPos``
		offsets [][]uint64 // documents offsets for each block (for block `blocks[i]` we will have offsets in `offsets[i]`)
		index   [][]int    // original position in `docsPos` (for document with offset from `offsets[i][j]` we will have its `docsPos` position in `index[i][j]`)
	)

	uniq := map[uint32]int{}

	for i, docPos := range docsPos {
		if docPos == DocPosNotFound {
			continue
		}

		block, offset := docPos.Unpack()

		b, ok := uniq[block]
		if !ok {
			b = len(blocks)
			uniq[block] = b
			index = append(index, nil)
			offsets = append(offsets, nil)
			blocks = append(blocks, block)
		}
		index[b] = append(index[b], i)
		offsets[b] = append(offsets[b], offset)
	}

	return blocks, offsets, index
}
