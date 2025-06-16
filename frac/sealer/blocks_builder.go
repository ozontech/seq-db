package sealer

import (
	"encoding/binary"
	"errors"
	"iter"

	"github.com/ozontech/seq-db/frac/ids"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/seq"
)

type BlocksBuilder struct {
	err error
}

// getTokensBlocks generates tokens blocks sequence from sorted tokens sequence
func getTokensBlocks(tokens iter.Seq[[]byte], blockSize int) iter.Seq[token.Block] {
	return func(yield func(token.Block) bool) {
		var (
			tid   uint32
			block token.Block
		)
		for token := range tokens {
			tid++
			if len(block.Payload) >= blockSize {
				if !yield(block) {
					return
				}
				// reset
				block.MinTID = tid
				block.Payload = block.Payload[:0]
				block.Offsets = block.Offsets[:0]
			}
			block.MaxTID = tid
			block.Offsets = append(block.Offsets, uint32(len(block.Payload)))
			block.Payload = binary.LittleEndian.AppendUint32(block.Payload, uint32(len(token)))
			block.Payload = append(block.Payload, token...)
		}
		yield(block)
	}
}

// TokensBlocksWithTokenTable generates a sequence of token blocks from a sorted sequence of tokens,
// and populates `tableBlock` with `token.TableEntry` data, using a `field` iterator providing the
// sorted sequence of fields.
func (bb *BlocksBuilder) TokensBlocksWithTokenTable(tokens iter.Seq[[]byte], fields iter.Seq2[string, uint32], blockSize int) (iter.Seq[token.Block], func() token.TableBlock) {
	bb.err = nil
	tableBlock := token.TableBlock{}
	return func(yield func(token.Block) bool) {
			nextBlock, stop := iter.Pull(getTokensBlocks(tokens, blockSize))
			defer stop()

			block, has := nextBlock() // get first block
			if !has {
				bb.err = errors.New("empty token blocks")
				return
			}
			if !yield(block) {
				return
			}

			var (
				blockIndex    uint32 = 1
				entryStartTID uint32 = 1
			)
			for field, fieldMaxTID := range fields {
				entries := []*token.TableEntry{}

				// for each field iterate needed blocks and calc entries (at least one needed)
				for needMoreEntries := true; needMoreEntries; {
					entries = append(entries, calcTokenTableEntry(entryStartTID, fieldMaxTID, blockIndex, block))

					// fieldMaxTID <  block.MaxTID 	: no more entries needed for current field and there is no need to load the next block
					// fieldMaxTID == block.MaxTID	: no more entries needed for current field, but the next block needs to be loaded
					// fieldMaxTID >  block.MaxTID	: more entries are needed for the current field and the next block needs to be loaded

					if fieldMaxTID < block.MaxTID {
						break
					}

					needMoreEntries = fieldMaxTID > block.MaxTID

					if block, has = nextBlock(); !has {
						if needMoreEntries {
							bb.err = errors.New("fields and tokens not consistent")
							return
						}
						break
					}
					if !yield(block) {
						return
					}
					blockIndex++
					entryStartTID = block.MinTID
				}
				entryStartTID = fieldMaxTID + 1

				tableBlock.FieldsTables = append(tableBlock.FieldsTables, token.FieldTable{
					Field:   field,
					Entries: entries,
				})
			}
			if _, has = nextBlock(); has {
				bb.err = errors.New("fields and tokens not consistent")
			}
		},
		func() token.TableBlock { return tableBlock }
}

func calcTokenTableEntry(entryStartTID, lastFieldTID, blockIndex uint32, block token.Block) *token.TableEntry {
	firstIndex := entryStartTID - block.MinTID
	lastIndex := min(lastFieldTID, block.MaxTID) - block.MinTID
	return &token.TableEntry{
		StartIndex: firstIndex,
		StartTID:   entryStartTID,
		BlockIndex: blockIndex,
		ValCount:   lastIndex - firstIndex + 1,
		MinVal:     string(block.GetToken(firstIndex)),
		MaxVal:     string(block.GetToken(lastIndex)),
	}
}

// IDsBlocks generates sequence of MIDs/RIDs/Positions blocks from IDs and DocPos
// iterators providing ordered sequence of ids and corresponding positions
func (bb *BlocksBuilder) IDsBlocks(seqIDs iter.Seq[seq.ID], docPos iter.Seq[seq.DocPos], blockSize int) iter.Seq[ids.Block] {
	bb.err = nil
	return func(yield func(ids.Block) bool) {
		var (
			has   bool
			pos   seq.DocPos
			block ids.Block
		)

		nextPos, stop := iter.Pull(docPos)
		defer stop()

		for id := range seqIDs {
			if len(block.MIDs) == blockSize {
				if !yield(block) {
					return
				}
				// reset
				block.MIDs = block.MIDs[:0]
				block.RIDs = block.RIDs[:0]
				block.Pos = block.Pos[:0]
			}
			if pos, has = nextPos(); !has {
				bb.err = errors.New("ids and pos not consistent")
				return
			}
			block.MIDs = append(block.MIDs, uint64(id.MID))
			block.RIDs = append(block.RIDs, uint64(id.RID))
			block.Pos = append(block.Pos, uint64(pos))
		}
		yield(block)
		if _, has = nextPos(); has {
			bb.err = errors.New("ids and pos not consistent")
		}
	}
}

// LIDsBlocks generates sequence of LIDs blocks from iterator of token lids providing ordered
// sequence of token lids sorted by TID and LID
func (bb *BlocksBuilder) LIDsBlocks(tokenLIDs iter.Seq[iter.Seq[uint32]], blockSize int) iter.Seq[lids.Block] {
	bb.err = nil
	return func(yield func(lids.Block) bool) {
		var (
			tid      uint32
			endOfTID bool
		)

		current := lids.Block{
			MinTID: 1,
			Chunks: lids.Chunks{
				LIDs:    make([]uint32, 0, blockSize),
				Offsets: []uint32{0},
			},
		}

		for lids := range tokenLIDs {
			tid++
			for lid := range lids {
				if endOfTID || len(current.Chunks.LIDs) == blockSize {
					current.Chunks.Offsets = append(current.Chunks.Offsets, uint32(len(current.Chunks.LIDs)))
					current.Chunks.IsLastLID = endOfTID // todo: get rid of IsLastLID
					if !yield(current) {
						return
					}
					endOfTID = false
					current.MinTID = tid
					current.Chunks.LIDs = current.Chunks.LIDs[:0]
					current.Chunks.Offsets = current.Chunks.Offsets[:1]
				}
				current.MaxTID = tid
				current.Chunks.LIDs = append(current.Chunks.LIDs, lid)
			}
			endOfTID = true
		}
		current.Chunks.Offsets = append(current.Chunks.Offsets, uint32(len(current.Chunks.LIDs)))
		yield(current)
	}
}
