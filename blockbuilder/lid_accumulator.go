package blockbuilder

import "github.com/ozontech/seq-db/frac/sealed/lids"

type LIDAccumulator struct {
	blockCapacity int
	onBlock       func(LidsSealBlock) error

	currentTID   uint32
	currentBlock LidsSealBlock

	isEndOfToken bool
	isContinued  bool
}

func NewLIDAccumulator(
	blockCapacity int,
	onBlock func(LidsSealBlock) error,
) *LIDAccumulator {
	a := &LIDAccumulator{
		blockCapacity: blockCapacity,
		onBlock:       onBlock,
	}

	a.currentBlock.Ext.MinTID = 1
	a.currentBlock.Payload = lids.Block{
		LIDs:    make([]uint32, 0, blockCapacity),
		Offsets: []uint32{0},
	}

	return a
}

// Add processes LIDs of one token (must be called in TID order).
//
// For each block that fills up, `onBlock` is called immediately
// before the backing arrays are reset, so `onBlock` may read the
// block data but must not retain references to it.
func (a *LIDAccumulator) Add(lidsbuf []uint32) error {
	a.currentTID++

	for _, lid := range lidsbuf {
		if len(a.currentBlock.Payload.LIDs) == a.blockCapacity {
			if err := a.onBlock(a.finalizeBlock()); err != nil {
				return err
			}

			a.currentBlock.Ext.MinTID = a.currentTID
			a.currentBlock.Payload.LIDs = a.currentBlock.Payload.LIDs[:0]
			a.currentBlock.Payload.Offsets = a.currentBlock.Payload.Offsets[:1]
		}

		a.isEndOfToken = false
		a.currentBlock.Ext.MaxTID = a.currentTID
		a.currentBlock.Payload.LIDs = append(a.currentBlock.Payload.LIDs, lid)
	}

	a.isEndOfToken = true
	a.currentBlock.Payload.Offsets = append(
		a.currentBlock.Payload.Offsets,
		uint32(len(a.currentBlock.Payload.LIDs)),
	)

	return nil
}

func (a *LIDAccumulator) Finalize() error {
	return a.onBlock(a.finalizeBlock())
}

func (a *LIDAccumulator) finalizeBlock() LidsSealBlock {
	if !a.isEndOfToken {
		a.currentBlock.Payload.Offsets = append(
			a.currentBlock.Payload.Offsets,
			uint32(len(a.currentBlock.Payload.LIDs)),
		)
	}

	result := a.currentBlock
	result.Payload.IsLastLID = a.isEndOfToken
	result.Ext.IsContinued = a.isContinued

	a.isContinued = !a.isEndOfToken
	return result
}
