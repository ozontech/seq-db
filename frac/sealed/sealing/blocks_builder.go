package sealing

import (
	"encoding/binary"
	"errors"
	"iter"

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

// BuildTokenBlocks generates a sequence of token blocks from a sorted sequence of tokens,
// and populates `tableBlock` with `token.TableEntry` data, using a `field` iterator providing
// the sorted sequence of fields.
//
// The relationship between fields, tokens, and table entries can be depicted:
//
// Fields Ranges:  		<-------f1----------><------f2-------><------------f3------------><----------f4---------->
// token.Block's: 		[.t1.t2.t3.t4.][.t5.t6.t7.t8.][.t9....etc...][.............][.............][.............]
// token.TableEntry's:  {-----f1------}{-f1-}{---f2--}{--f2--}{-f3--}{------f3-----}{-f3-}{----f4-}{-----f4------}

// Parameters:
//   - tokenBatches: Sequence of token batches, each batch becomes a block
//   - fields: Sequence of field names with their maximum token IDs
//
// Returns:
//   - iter.Seq[tokensSealBlock]: Sequence of sealed token blocks
//   - func() token.TableBlock: Function to retrieve the completed token table
func (bb *blocksBuilder) BuildTokenBlocks(
	tokenBatches iter.Seq[[][]byte],
	fields iter.Seq2[string, uint32],
) (iter.Seq[tokensSealBlock], func() token.TableBlock) {
	tableBlockBlock := token.TableBlock{} // Will contain all field table entries

	tokensBlockIterator := func(yield func(tokensSealBlock) bool) {
		// Create iterator for token blocks from source token data
		nextBlock, stop := iter.Pull(createTokensSealBlocks(tokenBatches))
		defer stop()

		// Process first block
		currentBlock, has := nextBlock()
		if !has {
			bb.lastErr = errors.New("sealing: empty token blocks provided")
			return
		}
		if !yield(currentBlock) {
			return
		}

		var (
			currentBlockIndex uint32 = 1 // 1-based block index for table entries
			entryStartTID     uint32 = 1 // Starting TID for current table entry
		)

		// Process each field definition
		for field, fieldMaxTID := range fields {
			entries := []*token.TableEntry{}

			// For each field at least one TableEntry must be defined.
			for needsMoreEntries := true; needsMoreEntries; {
				entry := createTokenTableEntry(entryStartTID, fieldMaxTID, currentBlockIndex, currentBlock)
				entries = append(entries, entry)

				// A field may span multiple token blocks. Determine if field continues into next block:
				// - fieldMaxTID < currentBlock.MaxTID: field ends in current block
				// - fieldMaxTID == currentBlock.MaxTID: field ends exactly at block boundary
				// - fieldMaxTID > currentBlock.MaxTID: field continues to next block
				if fieldMaxTID < currentBlock.ext.maxTID {
					break
				}

				needsMoreEntries = fieldMaxTID > currentBlock.ext.maxTID

				// Load next block for continuing fields
				if currentBlock, has = nextBlock(); !has {
					if needsMoreEntries {
						bb.lastErr = errors.New("sealing: fields and tokens not consistent - unexpected end of token blocks")
						return
					}
					break
				}
				if !yield(currentBlock) {
					return
				}
				currentBlockIndex++
				entryStartTID = currentBlock.ext.minTID
			}
			entryStartTID = fieldMaxTID + 1

			// Add completed field table
			tableBlockBlock.FieldsTables = append(tableBlockBlock.FieldsTables, token.FieldTable{
				Field:   field,
				Entries: entries,
			})
		}

		// Verify no excess token blocks remain
		if _, has = nextBlock(); has {
			bb.lastErr = errors.New("sealing: fields and tokens not consistent - excess token blocks after processing all fields")
		}
	}

	// Closure to access the completed token table
	tokenTableBlockProvider := func() token.TableBlock { return tableBlockBlock }

	return tokensBlockIterator, tokenTableBlockProvider
}

// createTokenTableEntry creates a token table entry for a field-block span.
// Calculates the range of tokens belonging to a field within a specific block.
// Parameters:
//   - entryStartTID: Starting token ID for this entry
//   - fieldMaxTID: Maximum token ID for the field
//   - blockIndex: Index of the current token block
//   - block: Current token block data
func createTokenTableEntry(entryStartTID, fieldMaxTID, blockIndex uint32, block tokensSealBlock) *token.TableEntry {
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

// BuildLIDsBlocks constructs LID blocks from Token LID sequences.
// Processes LIDs grouped by Token ID and creates optimally sized blocks:
// - Maintains LID order within each token ID
// - Splits large LID sequences across multiple blocks
// - Tracks continuation status between blocks
// - Preserves token ID to LID mapping relationships
//
// Parameters:
//   - tokenLIDs: Sequence of LID arrays, one per TokenID, in TID order
//   - blockCapacity: Maximum number of LIDs per block
//
// Returns:
//   - iter.Seq[lidsSealBlock]: Sequence of sealed LID blocks
func (bb *blocksBuilder) BuildLIDsBlocks(tokenLIDs iter.Seq[[]uint32], blockCapacity int) iter.Seq[lidsSealBlock] {
	return func(yield func(lidsSealBlock) bool) {
		var (
			currentTID    uint32        // Current TID being processed
			currentBlock  lidsSealBlock // Current block under construction
			isEndOfToken  bool          // Flag for end of current token's LIDs
			continuesNext bool          // Flag for block continuation
		)

		// Initialize first block
		currentBlock.ext.minTID = 1
		currentBlock.payload = lids.Block{
			LIDs:    make([]uint32, 0, blockCapacity), // Pre-allocate with capacity
			Offsets: []uint32{0},                      // Start with initial offset
		}

		// finalizeBlock prepares and yields the current block
		finalizeBlock := func() bool {
			if !isEndOfToken {
				// Add final offset for current token if not already done
				currentBlock.payload.Offsets = append(currentBlock.payload.Offsets, uint32(len(currentBlock.payload.LIDs)))
			}
			currentBlock.payload.IsLastLID = isEndOfToken // TODO: Remove legacy field
			currentBlock.ext.isContinued = continuesNext  // TODO: Remove legacy field
			continuesNext = !isEndOfToken
			return yield(currentBlock)
		}

		// Process LIDs for each token ID
		for lidsBatch := range tokenLIDs {
			currentTID++

			// Add each LID for current token to the block
			for _, lid := range lidsBatch {
				// Check if block reached capacity
				if len(currentBlock.payload.LIDs) == blockCapacity {
					if !finalizeBlock() {
						return
					}
					// Initialize new block
					currentBlock.ext.minTID = currentTID
					currentBlock.payload.LIDs = currentBlock.payload.LIDs[:0]
					currentBlock.payload.Offsets = currentBlock.payload.Offsets[:1] // Reset to initial offset
				}

				isEndOfToken = false
				currentBlock.ext.maxTID = currentTID
				currentBlock.payload.LIDs = append(currentBlock.payload.LIDs, lid)
			}

			// Mark end of current token and store offset
			currentBlock.payload.Offsets = append(currentBlock.payload.Offsets, uint32(len(currentBlock.payload.LIDs)))
			isEndOfToken = true
		}

		// Yield the final block
		finalizeBlock()
	}
}

// createIDsSealBlocks converts sequences of IDs and positions into sealed ID blocks.
// Transforms raw ID sequences into optimized block format for storage:
// - Processes IDs in batches for efficiency
// - Maintains correlation between IDs and their positions
// - Creates separate blocks for MIDs, RIDs, and positions
//
// Parameters:
//   - idsBatches: Sequence of ID batches with corresponding document positions
//
// Returns:
//   - iter.Seq[idsSealBlock]: Sequence of sealed ID blocks
func createIDsSealBlocks(idsBatches iter.Seq2[[]seq.ID, []seq.DocPos]) iter.Seq[idsSealBlock] {
	return func(yield func(idsSealBlock) bool) {
		block := idsSealBlock{}

		// Process each batch of IDs and positions
		for ids, positions := range idsBatches {
			// Reset block arrays for new batch
			block.mids.Values = block.mids.Values[:0]
			block.rids.Values = block.rids.Values[:0]
			block.params.Values = block.params.Values[:0]

			// Convert each ID and position to storage format
			for i, id := range ids {
				block.mids.Values = append(block.mids.Values, uint64(id.MID))
				block.rids.Values = append(block.rids.Values, uint64(id.RID))
				block.params.Values = append(block.params.Values, uint64(positions[i]))
			}

			// Yield completed block
			if !yield(block) {
				return
			}
		}
	}
}

// createTokensSealBlocks converts raw token sequences into sealed token blocks.
// Transforms batches of tokens into optimized storage format:
// - Merges a set of byte slices into a contiguous slice (Payload) and a slice of offsets (Offsets)
// - Tracks token ID ranges for indexing [MinTID, MaxTID]
//
// Parameters:
//   - tokenBatches: Sequence of token batches to process
//
// Returns:
//   - iter.Seq[tokensSealBlock]: Sequence of sealed token blocks
func createTokensSealBlocks(tokenBatches iter.Seq[[][]byte]) iter.Seq[tokensSealBlock] {
	return func(yield func(tokensSealBlock) bool) {
		var (
			currentTID uint32          // Current token ID counter
			block      tokensSealBlock // Current block under construction
		)

		// Process each batch of tokens
		for tokens := range tokenBatches {
			// Initialize new block
			block.ext.minTID = currentTID + 1
			block.payload.Payload = block.payload.Payload[:0]
			block.payload.Offsets = block.payload.Offsets[:0]

			// Process each token in current batch
			for _, tokenData := range tokens {
				currentTID++
				// Store offset to current token
				block.payload.Offsets = append(block.payload.Offsets, uint32(len(block.payload.Payload)))
				// Store token length (little-endian) followed by token bytes
				block.payload.Payload = binary.LittleEndian.AppendUint32(block.payload.Payload, uint32(len(tokenData)))
				block.payload.Payload = append(block.payload.Payload, tokenData...)
			}

			block.ext.maxTID = currentTID

			// Yield completed block
			if !yield(block) {
				return
			}
		}
	}
}
