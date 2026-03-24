package sealing

import (
	"encoding/binary"
	"errors"
	"iter"
	"unsafe"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
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
type blocksBuilder struct {
	lastErr error // Last error encountered during processing
}

// LastError returns the last error encountered during block processing.
func (bb *blocksBuilder) LastError() error {
	return bb.lastErr
}

// BuildTokenBlocks converts scalar (token, lids) pairs into token blocks with field tables.
// onLIDs is called for each token's LIDs immediately during iteration — the caller must not
// retain the slice after onLIDs returns. Errors from onLIDs are stored in bb.lastErr.
//
// Visualization of relationships between fields, tokens, and table entries:
//
// Field Ranges:    <-------f1----------><------f2-------><------------f3------------><----------f4---------->
// Token Blocks:    [.t1.t2.t3.t4.][.t5.t6.t7.t8.][.t9....etc...][.............][.............][.............]
// Field Entries:   {-----f1------}{-f1-}{---f2--}{--f2--}{-f3--}{------f3-----}{-f3-}{----f4-}{-----f4------}
//
// Parameters:
//   - tokens: Scalar sequence of (token bytes, per-token LID list) pairs
//   - fields: Iterator of [fieldName, maxTID] pairs for all fields in ascending TID order
//   - blockSize: Maximum payload size in bytes per token block
//   - onLIDs: Called for each token's LIDs before the source advances to the next token
func (bb *blocksBuilder) BuildTokenBlocks(
	tokens iter.Seq2[[]byte, []uint32],
	fields iter.Seq2[string, uint32],
	accumulate func([]uint32) error,
	blockSize int,
) iter.Seq2[tokensSealBlock, []token.FieldTable] {
	return func(yield func(tokensSealBlock, []token.FieldTable) bool) {
		nextField, stop := iter.Pull2(fields)
		defer stop()

		var (
			hasMore     bool
			currentTID  uint32 = 1
			fieldMaxTID uint32 = 0
			fieldName   string
		)

		// Just wrap `accumulate` function to be able
		// to track returned errors.
		accumulate := func(lids []uint32) error {
			if err := accumulate(lids); err != nil {
				bb.lastErr = err
				return err
			}
			return nil
		}

		for blockIdx, block := range seqBlockToken(tokens, blockSize, accumulate) {
			if bb.lastErr != nil {
				return
			}

			// A block may span multiple fields, and a field may span multiple blocks.
			// We emit one TableEntry per (field, block) intersection so that lookups
			// can find the exact position of any token given its field and TID.
			var table []token.FieldTable
			for currentTID <= block.ext.maxTID {
				if fieldMaxTID < currentTID {
					if fieldName, fieldMaxTID, hasMore = nextField(); !hasMore {
						bb.lastErr = errors.New("not enough fields to cover all TIDs")
						return
					}
				}

				entry := newTokenTableEntry(currentTID, fieldMaxTID, blockIdx, block)
				currentTID += entry.ValCount

				table = append(table, token.FieldTable{
					Field:   fieldName,
					Entries: []*token.TableEntry{entry}},
				)
			}

			if !yield(block, table) {
				return
			}
		}

		if bb.lastErr != nil {
			return
		}

		if currentTID-1 != fieldMaxTID {
			bb.lastErr = errors.New("fields and tokens not consistent")
		} else if _, _, hasMore = nextField(); hasMore {
			bb.lastErr = errors.New("excess field after processing all blocks")
		}
	}
}

func newTokenTableEntry(
	entryStartTID, fieldMaxTID,
	blockIndex uint32, block tokensSealBlock,
) *token.TableEntry {
	// Convert global TIDs to block-local indices
	firstIndex := entryStartTID - block.ext.minTID
	lastIndex := min(fieldMaxTID, block.ext.maxTID) - block.ext.minTID

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
// A new block is yielded every `blockSize` IDs.
func seqBlockID(
	ids iter.Seq2[seq.ID, seq.DocPos],
	blockSize int,
) iter.Seq[idsSealBlock] {
	return func(yield func(idsSealBlock) bool) {
		var block idsSealBlock

		for id, pos := range ids {
			block.mids.Values = append(block.mids.Values, uint64(id.MID))
			block.rids.Values = append(block.rids.Values, uint64(id.RID))
			block.params.Values = append(block.params.Values, uint64(pos))

			if len(block.mids.Values) == blockSize {
				if !yield(block) {
					return
				}

				block.mids.Values = block.mids.Values[:0]
				block.rids.Values = block.rids.Values[:0]
				block.params.Values = block.params.Values[:0]
			}
		}

		if len(block.mids.Values) > 0 {
			yield(block)
		}
	}
}

// seqBlockToken accumulates scalar (token, lids) pairs into sealed token blocks.
// A new block is started whenever the accumulated payload would exceed blockSize bytes.
// onLIDs is called for each token's LIDs immediately during iteration — the caller must not
// retain the slice after onLIDs returns. If onLIDs returns a non-nil error, iteration stops.
//
// Parameters:
//   - tokens: Scalar sequence of (token bytes, per-token LID list) pairs
//   - blockSize: Maximum payload size in bytes before starting a new block
//   - onLIDs: Called for each token's LIDs before the source advances to the next token
//
// Returns:
//   - iter.Seq2[uint32, tokensSealBlock]: Sequence of (block index, sealed token block) pairs
func seqBlockToken(
	tokens iter.Seq2[[]byte, []uint32],
	blockSize int, accumulate func([]uint32) error,
) iter.Seq2[uint32, tokensSealBlock] {
	return func(yield func(uint32, tokensSealBlock) bool) {
		var (
			idx        uint32          // 0-based block index
			currentTID uint32          // monotonically increasing TID
			block      tokensSealBlock // block under construction
			actualSize int             // accumulated payload bytes
		)

		block.ext.minTID = 1
		flush := func() bool {
			block.ext.maxTID = currentTID

			if !yield(idx, block) {
				return false
			}

			idx++

			// We yielded complete token block several lines earlier.
			// And now we prepare token block for the next batch.
			block.payload.Payload = block.payload.Payload[:0]
			block.payload.Offsets = block.payload.Offsets[:0]

			// Here we increment currentTID by one because
			// it points to TID at the end of the *currently* yielded block.
			block.ext.minTID = currentTID + 1

			actualSize = 0
			return true
		}

		for token, lids := range tokens {
			// We encode token as [size](4B)[token](?B).
			tokenSize := int(unsafe.Sizeof(uint32(0))) + len(token)

			needsFlushing := actualSize > 0 &&
				actualSize+tokenSize > blockSize

			if needsFlushing {
				if !flush() {
					return
				}
			}

			block.payload.Offsets = append(
				block.payload.Offsets,
				uint32(len(block.payload.Payload)),
			)

			block.payload.Payload = binary.LittleEndian.AppendUint32(
				block.payload.Payload,
				uint32(len(token)),
			)

			block.payload.Payload = append(
				block.payload.Payload,
				token...,
			)

			if err := accumulate(lids); err != nil {
				return
			}

			currentTID += 1
			actualSize += tokenSize
		}

		if actualSize > 0 {
			flush()
		}
	}
}

// lidBlocksAcc incrementally builds LID blocks from per-token LID lists.
// Call Add for each token's LIDs in TID order, passing a callback that is invoked
// for each completed block before its backing arrays are reused.
// Call Flush once after all Add calls to handle the final (possibly partial) block.
type lidBlocksAcc struct {
	blockCap     int
	currentTID   uint32
	currentBlock lidsSealBlock
	isEndOfToken bool
	isContinued  bool
}

func newLIDBlocksAccumulator(blockCap int) *lidBlocksAcc {
	a := &lidBlocksAcc{blockCap: blockCap}
	a.currentBlock.ext.minTID = 1
	a.currentBlock.payload = lids.Block{
		LIDs:    make([]uint32, 0, blockCap),
		Offsets: []uint32{0},
	}
	return a
}

// Add processes LIDs of one token (must be called in TID order).
//
// For each block that fills up, `onBlock` is called immediately
// before the backing arrays are reset, so `onBlock` may read the
// block data but must not retain references to it.
func (a *lidBlocksAcc) Add(lids []uint32, onBlock func(lidsSealBlock) error) error {
	a.currentTID++

	for _, lid := range lids {
		if len(a.currentBlock.payload.LIDs) == a.blockCap {
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
