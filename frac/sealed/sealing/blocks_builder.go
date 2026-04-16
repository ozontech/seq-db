package sealing

import (
	"encoding/binary"
	"iter"
	"unsafe"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/util"
)

type (
	TokenBlock = util.Pair[tokensSealBlock, []token.FieldTable]
)

// tokensExt represents the token ID range contained in a block.
type tokensExt struct {
	minTID uint32 // First token ID in the block
	maxTID uint32 // Last token ID in the block
}

// tokensSealBlock represents a sealed block containing token data with metadata.
type tokensSealBlock struct {
	ext     tokensExt   // Tokens block metadata for registry marking
	payload token.Block // Actual token data payload
}

// lidsExt represents the range and continuation status of LID blocks.
type lidsExt struct {
	minTID      uint32 // First token ID in the LID block
	maxTID      uint32 // Last token ID in the LID block
	isContinued bool   // Whether LID sequence continues in next block
}

// lidsSealBlock represents a sealed block containing LID (Local ID) data.
type lidsSealBlock struct {
	ext     lidsExt    // LIDs block metadata for registry marking
	payload lids.Block // LID data payload
}

// idsSealBlock represents a sealed block containing various identifier types.
type idsSealBlock struct {
	mids   seqids.BlockMIDs
	rids   seqids.BlockRIDs
	params seqids.BlockParams
}

// blocksBuilder constructs sealed blocks from various data sources.
// Provides error tracking and consistency validation during block construction.
type blocksBuilder struct{}

func (bb *blocksBuilder) BuildTokenBlocks(
	it iter.Seq2[string, iter.Seq2[TokenPosting, error]],
	accumulate func([]uint32) error, blockCapacity int,
) iter.Seq2[TokenBlock, error] {
	return func(yield func(TokenBlock, error) bool) {
		var (
			block     tokensSealBlock
			blockIdx  uint32
			blockSize int
		)

		var (
			currentTID         uint32
			pendingTable       []token.FieldTable
			fieldName          string
			fieldEntryStartTID uint32
		)

		emitFieldEntry := func() {
			// Handle case when field does not have tokens.
			if fieldName == "" || fieldEntryStartTID > currentTID {
				return
			}

			entry := newTokenTableEntry(fieldEntryStartTID, currentTID, blockIdx, block)
			pendingTable = append(pendingTable, token.FieldTable{
				Field:   fieldName,
				Entries: []*token.TableEntry{entry},
			})
		}

		flushBlock := func() bool {
			emitFieldEntry()
			block.ext.maxTID = currentTID

			pair := TokenBlock{First: block, Second: pendingTable}
			if !yield(pair, nil) {
				return false
			}

			block.payload.Payload = block.payload.Payload[:0]
			block.payload.Offsets = block.payload.Offsets[:0]
			block.ext.minTID = currentTID + 1

			blockIdx++
			blockSize = 0

			pendingTable = pendingTable[:0]
			fieldEntryStartTID = currentTID + 1

			return true
		}

		block.ext.minTID = 1
		for field, tokIt := range it {
			emitFieldEntry()

			fieldName = field
			fieldEntryStartTID = currentTID + 1

			for pair, err := range tokIt {
				if err != nil {
					yield(TokenBlock{}, err)
					return
				}

				tok, tlids := pair.First, pair.Second
				tokenSize := int(unsafe.Sizeof(uint32(0))) + len(tok)

				if blockSize > 0 && blockSize+tokenSize > blockCapacity {
					if !flushBlock() {
						return
					}
				}

				block.payload.Offsets = append(block.payload.Offsets, uint32(len(block.payload.Payload)))
				block.payload.Payload = binary.LittleEndian.AppendUint32(block.payload.Payload, uint32(len(tok)))
				block.payload.Payload = append(block.payload.Payload, tok...)

				if err := accumulate(tlids); err != nil {
					yield(TokenBlock{}, err)
					return
				}

				currentTID++
				blockSize += tokenSize
			}
		}

		if blockSize > 0 {
			flushBlock()
		}
	}
}

func newTokenTableEntry(
	entryStartTID, entryEndTID uint32,
	blockIndex uint32, block tokensSealBlock,
) *token.TableEntry {
	// Convert global TIDs to block-local indices
	firstIndex := entryStartTID - block.ext.minTID
	lastIndex := entryEndTID - block.ext.minTID

	// Extract min and max token values for the entry range
	minVal := string(block.payload.GetToken(int(firstIndex)))
	maxVal := string(block.payload.GetToken(int(lastIndex)))

	return &token.TableEntry{
		StartIndex: firstIndex,                 // Starting index within the block
		StartTID:   entryStartTID,              // Starting token ID (global)
		BlockIndex: blockIndex,                 // Reference to containing block
		ValCount:   lastIndex - firstIndex + 1, // Number of tokens in this entry
		MinVal:     minVal,                     // Smallest token value in range
		MaxVal:     maxVal,                     // Largest token value in range
	}
}

// seqBlockID accumulates scalar (ID, position) pairs into sealed ID blocks.
// A new block is yielded every `blockCapacity` IDs.
func seqBlockID(ids iter.Seq2[DocLocation, error], blockCapacity int) iter.Seq2[idsSealBlock, error] {
	return func(yield func(idsSealBlock, error) bool) {
		var block idsSealBlock

		for pair, err := range ids {
			if err != nil {
				yield(idsSealBlock{}, err)
				return
			}

			id, pos := pair.First, pair.Second
			block.mids.Values = append(block.mids.Values, uint64(id.MID))
			block.rids.Values = append(block.rids.Values, uint64(id.RID))
			block.params.Values = append(block.params.Values, uint64(pos))

			if len(block.mids.Values) == blockCapacity {
				if !yield(block, nil) {
					return
				}

				block.mids.Values = block.mids.Values[:0]
				block.rids.Values = block.rids.Values[:0]
				block.params.Values = block.params.Values[:0]
			}
		}

		if len(block.mids.Values) > 0 {
			yield(block, nil)
		}
	}
}

type lidBlocksAcc struct {
	blockCapacity int

	currentTID   uint32
	currentBlock lidsSealBlock

	isEndOfToken bool
	isContinued  bool
}

func newLIDBlocksAccumulator(blockCapacity int) *lidBlocksAcc {
	a := &lidBlocksAcc{blockCapacity: blockCapacity}

	a.currentBlock.ext.minTID = 1
	a.currentBlock.payload = lids.Block{
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
func (a *lidBlocksAcc) Add(lidsbuf []uint32, onBlock func(lidsSealBlock) error) error {
	a.currentTID++

	for _, lid := range lidsbuf {
		if len(a.currentBlock.payload.LIDs) == a.blockCapacity {
			if err := onBlock(a.finalizeBlock()); err != nil {
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

func (a *lidBlocksAcc) Flush() lidsSealBlock {
	return a.finalizeBlock()
}

func (a *lidBlocksAcc) finalizeBlock() lidsSealBlock {
	if !a.isEndOfToken {
		a.currentBlock.payload.Offsets = append(
			a.currentBlock.payload.Offsets,
			uint32(len(a.currentBlock.payload.LIDs)),
		)
	}

	result := a.currentBlock
	result.payload.IsLastLID = a.isEndOfToken
	result.ext.isContinued = a.isContinued

	a.isContinued = !a.isEndOfToken
	return result
}
