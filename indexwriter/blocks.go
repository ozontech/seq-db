package indexwriter

import (
	"encoding/binary"
	"iter"
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/util"
)

type tokenFieldBlock = util.Pair[unpackedTokenBlock, []token.FieldTable]

// tokenExt represents the token ID range contained in a block.
type tokenExt struct {
	minTID uint32 // First token ID in the block
	maxTID uint32 // Last token ID in the block
}

// unpackedTokenBlock represents a sealed block containing token data with metadata.
type unpackedTokenBlock struct {
	ext     tokenExt    // Tokens block metadata for registry marking
	payload token.Block // Actual token data payload
}

// lidExt represents the range and continuation status of LID blocks.
type lidExt struct {
	minTID      uint32 // First token ID in the LID block
	maxTID      uint32 // Last token ID in the LID block
	isContinued bool   // Whether LID sequence continues in next block
}

// unpackedLIDBlock represents a sealed block containing LID (Local ID) data.
type unpackedLIDBlock struct {
	ext     lidExt     // LIDs block metadata for registry marking
	payload lids.Block // LID data payload
}

// unpackedIDBlock represents a sealed block containing various identifier types.
type unpackedIDBlock struct {
	mids   seqids.BlockMIDs
	rids   seqids.BlockRIDs
	params seqids.BlockParams
}

func tokenBlock(
	it iter.Seq2[string, iter.Seq2[TokenLIDs, error]],
	accumulate func([]uint32) error, blockCapacity int, tokenFreqAbsThreshold int,
) iter.Seq2[tokenFieldBlock, error] {
	return func(yield func(tokenFieldBlock, error) bool) {
		var (
			block     unpackedTokenBlock
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

			pair := tokenFieldBlock{First: block, Second: pendingTable}
			if !yield(pair, nil) {
				return false
			}

			block.payload.Payload = block.payload.Payload[:0]
			block.payload.Offsets = block.payload.Offsets[:0]
			block.payload.FreqIndexes = block.payload.FreqIndexes[:0]
			block.payload.Freqs = block.payload.Freqs[:0]
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
					yield(tokenFieldBlock{}, err)
					return
				}

				tok, tlids := pair.First, pair.Second
				tokenSize := int(unsafe.Sizeof(uint32(0))) + len(tok)

				if blockSize > 0 && blockSize+tokenSize > blockCapacity {
					if !flushBlock() {
						return
					}
				}

				tokenIndex := uint32(len(block.payload.Offsets))
				block.payload.Offsets = append(block.payload.Offsets, uint32(len(block.payload.Payload)))
				block.payload.Payload = binary.LittleEndian.AppendUint32(block.payload.Payload, uint32(len(tok)))
				block.payload.Payload = append(block.payload.Payload, tok...)

				if len(tlids) >= tokenFreqAbsThreshold {
					if tokenIndex > math.MaxUint16 {
						panic("unsupported token block size")
					}
					block.payload.FreqIndexes = append(block.payload.FreqIndexes, uint16(tokenIndex))
					block.payload.Freqs = append(block.payload.Freqs, uint32(len(tlids)))
				}

				if err := accumulate(tlids); err != nil {
					yield(tokenFieldBlock{}, err)
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
	blockIndex uint32, block unpackedTokenBlock,
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
		Letters:    block.payload.LettersBitset(),
	}
}

// idBlock accumulates scalar (ID, position) pairs into sealed ID blocks.
// A new block is yielded every `blockCapacity` IDs.
func idBlock(ids iter.Seq2[DocLocation, error], blockCapacity int) iter.Seq2[unpackedIDBlock, error] {
	return func(yield func(unpackedIDBlock, error) bool) {
		var block unpackedIDBlock

		for pair, err := range ids {
			if err != nil {
				yield(unpackedIDBlock{}, err)
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

// collapseOrderedFieldsTables merges FieldTables with the same field name.
// Assumes input is sorted by Field.
func collapseOrderedFieldsTables(src []token.FieldTable) []token.FieldTable {
	if len(src) == 0 {
		return nil
	}

	current := src[0]
	var dst []token.FieldTable
	for _, ft := range src[1:] {
		if current.Field == ft.Field {
			current.Entries = append(current.Entries, ft.Entries...)
			continue
		}

		dst = append(dst, current)
		current = ft
	}

	return append(dst, current)
}
