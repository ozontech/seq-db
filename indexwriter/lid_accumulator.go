package indexwriter

import (
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/lids"
)

type lidAccumulator struct {
	blockCapacity int
	onBlock       func(unpackedLIDBlock) error

	currentTID   uint32
	currentBlock unpackedLIDBlock

	isEndOfToken bool
}

func newLIDAccumulator(
	blockCapacity int,
	onBlock func(unpackedLIDBlock) error,
) *lidAccumulator {
	if blockCapacity == 0 {
		blockCapacity = consts.DefaultLIDBlockCap
	}

	a := &lidAccumulator{
		blockCapacity: blockCapacity,
		onBlock:       onBlock,
	}

	a.currentBlock.ext.minTID = 1
	a.currentBlock.payload = lids.Block{
		LIDs:    make([]uint32, 0, blockCapacity),
		Offsets: []uint32{0},
	}

	return a
}

// add processes LIDs of one token (must be called in TID order).
//
// For each block that fills up, `onBlock` is called immediately
// before the backing arrays are reset, so `onBlock` may read the
// block data but must not retain references to it.
func (a *lidAccumulator) add(lidsbuf []uint32) error {
	a.currentTID++

	for _, lid := range lidsbuf {
		if len(a.currentBlock.payload.LIDs) == a.blockCapacity {
			if err := a.onBlock(a.finalizeBlock()); err != nil {
				return err
			}

			a.currentBlock.ext.minTID = a.currentTID
			a.currentBlock.payload.LIDs = a.currentBlock.payload.LIDs[:0]
			a.currentBlock.payload.Offsets = a.currentBlock.payload.Offsets[:1]
		}

		a.isEndOfToken = false
		a.currentBlock.ext.maxTID = a.currentTID
		a.currentBlock.payload.LIDs = append(a.currentBlock.payload.LIDs, lid)
	}

	a.isEndOfToken = true
	a.currentBlock.payload.Offsets = append(
		a.currentBlock.payload.Offsets,
		uint32(len(a.currentBlock.payload.LIDs)),
	)

	return nil
}

func (a *lidAccumulator) finalize() error {
	return a.onBlock(a.finalizeBlock())
}

func (a *lidAccumulator) finalizeBlock() unpackedLIDBlock {
	if !a.isEndOfToken {
		a.currentBlock.payload.Offsets = append(
			a.currentBlock.payload.Offsets,
			uint32(len(a.currentBlock.payload.LIDs)),
		)
	}

	result := a.currentBlock
	if blockLIDs := result.payload.LIDs; len(blockLIDs) > 0 {
		result.ext.firstLID = blockLIDs[0]
		result.ext.lastLID = blockLIDs[len(blockLIDs)-1]
	}

	return result
}
