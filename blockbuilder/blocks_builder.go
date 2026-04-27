package blockbuilder

import (
	"encoding/binary"
	"iter"
	"unsafe"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type (
	DocLocation  = util.Pair[seq.ID, seq.DocPos]
	TokenPosting = util.Pair[[]byte, []uint32]
	TokenBlock   = util.Pair[TokensSealBlock, []token.FieldTable]
)

// TokensExt represents the token ID range contained in a block.
type TokensExt struct {
	MinTID uint32 // First token ID in the block
	MaxTID uint32 // Last token ID in the block
}

// TokensSealBlock represents a sealed block containing token data with metadata.
type TokensSealBlock struct {
	Ext     TokensExt   // Tokens block metadata for registry marking
	Payload token.Block // Actual token data payload
}

// LidsExt represents the range and continuation status of LID blocks.
type LidsExt struct {
	MinTID      uint32 // First token ID in the LID block
	MaxTID      uint32 // Last token ID in the LID block
	IsContinued bool   // Whether LID sequence continues in next block
}

// LidsSealBlock represents a sealed block containing LID (Local ID) data.
type LidsSealBlock struct {
	Ext     LidsExt    // LIDs block metadata for registry marking
	Payload lids.Block // LID data payload
}

// IdsSealBlock represents a sealed block containing various identifier types.
type IdsSealBlock struct {
	MIDs   seqids.BlockMIDs
	RIDs   seqids.BlockRIDs
	Params seqids.BlockParams
}

// BlocksBuilder constructs sealed blocks from various data sources.
type BlocksBuilder struct{}

func (bb *BlocksBuilder) BuildTokenBlocks(
	it iter.Seq2[string, iter.Seq2[TokenPosting, error]],
	accumulate func([]uint32) error, blockCapacity int,
) iter.Seq2[TokenBlock, error] {
	return func(yield func(TokenBlock, error) bool) {
		var (
			block     TokensSealBlock
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
			block.Ext.MaxTID = currentTID

			pair := TokenBlock{First: block, Second: pendingTable}
			if !yield(pair, nil) {
				return false
			}

			block.Payload.Payload = block.Payload.Payload[:0]
			block.Payload.Offsets = block.Payload.Offsets[:0]
			block.Ext.MinTID = currentTID + 1

			blockIdx++
			blockSize = 0

			pendingTable = pendingTable[:0]
			fieldEntryStartTID = currentTID + 1

			return true
		}

		block.Ext.MinTID = 1
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

				block.Payload.Offsets = append(block.Payload.Offsets, uint32(len(block.Payload.Payload)))
				block.Payload.Payload = binary.LittleEndian.AppendUint32(block.Payload.Payload, uint32(len(tok)))
				block.Payload.Payload = append(block.Payload.Payload, tok...)

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
	blockIndex uint32, block TokensSealBlock,
) *token.TableEntry {
	// Convert global TIDs to block-local indices
	firstIndex := entryStartTID - block.Ext.MinTID
	lastIndex := entryEndTID - block.Ext.MinTID

	// Extract min and max token values for the entry range
	minVal := string(block.Payload.GetToken(int(firstIndex)))
	maxVal := string(block.Payload.GetToken(int(lastIndex)))

	return &token.TableEntry{
		StartIndex: firstIndex,                 // Starting index within the block
		StartTID:   entryStartTID,              // Starting token ID (global)
		BlockIndex: blockIndex,                 // Reference to containing block
		ValCount:   lastIndex - firstIndex + 1, // Number of tokens in this entry
		MinVal:     minVal,                     // Smallest token value in range
		MaxVal:     maxVal,                     // Largest token value in range
	}
}

// SeqBlockID accumulates scalar (ID, position) pairs into sealed ID blocks.
// A new block is yielded every `blockCapacity` IDs.
func SeqBlockID(ids iter.Seq2[DocLocation, error], blockCapacity int) iter.Seq2[IdsSealBlock, error] {
	return func(yield func(IdsSealBlock, error) bool) {
		var block IdsSealBlock

		for pair, err := range ids {
			if err != nil {
				yield(IdsSealBlock{}, err)
				return
			}

			id, pos := pair.First, pair.Second
			block.MIDs.Values = append(block.MIDs.Values, uint64(id.MID))
			block.RIDs.Values = append(block.RIDs.Values, uint64(id.RID))
			block.Params.Values = append(block.Params.Values, uint64(pos))

			if len(block.MIDs.Values) == blockCapacity {
				if !yield(block, nil) {
					return
				}

				block.MIDs.Values = block.MIDs.Values[:0]
				block.RIDs.Values = block.RIDs.Values[:0]
				block.Params.Values = block.Params.Values[:0]
			}
		}

		if len(block.MIDs.Values) > 0 {
			yield(block, nil)
		}
	}
}

// LidBlocksAcc accumulates LIDs into sealed LID blocks.
type LidBlocksAcc struct {
	blockCapacity int

	currentTID   uint32
	currentBlock LidsSealBlock

	isEndOfToken bool
	isContinued  bool
}

func NewLIDBlocksAccumulator(blockCapacity int) *LidBlocksAcc {
	a := &LidBlocksAcc{blockCapacity: blockCapacity}

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
func (a *LidBlocksAcc) Add(lidsbuf []uint32, onBlock func(LidsSealBlock) error) error {
	a.currentTID++

	for _, lid := range lidsbuf {
		if len(a.currentBlock.Payload.LIDs) == a.blockCapacity {
			if err := onBlock(a.finalizeBlock()); err != nil {
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

func (a *LidBlocksAcc) Flush() LidsSealBlock {
	return a.finalizeBlock()
}

func (a *LidBlocksAcc) finalizeBlock() LidsSealBlock {
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

// CollapseOrderedFieldsTables merges FieldTables with the same field name.
// Assumes input is sorted by Field.
func CollapseOrderedFieldsTables(src []token.FieldTable) []token.FieldTable {
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
