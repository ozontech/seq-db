package sealing

import (
	"encoding/binary"
	"errors"
	"iter"

	"github.com/ozontech/seq-db/frac/sealed/ids"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
)

type tokensExt struct {
	minTID uint32
	maxTID uint32
}

type tokensSealBlock struct {
	// ext describes content of block and needs for registry marking (ext1/ext2 and etc.)
	ext     tokensExt   // and creation of auxiliary structures (token.TableBlock)
	payload token.Block // data directly contains the tokens block
}

type lidsExt struct {
	minTID      uint32
	maxTID      uint32
	isContinued bool
}

type lidsSealBlock struct {
	ext     lidsExt    // ext describes content of block and needs for registry marking (ext1/ext2 and etc.)
	payload lids.Block // data directly contains the lids block
}

type idsSealBlock struct {
	mids   ids.BlockMIDs
	rids   ids.BlockRIDs
	params ids.BlockParams
}

type blocksBuilder struct {
	lastErr error
}

// TokensBlocksWithTokenTable generates a sequence of token blocks from a sorted sequence of tokens,
// and populates `tableBlock` with `token.TableEntry` data, using a `field` iterator providing the
// sorted sequence of fields.
func (bb *blocksBuilder) TokensBlocksWithTokenTable(tokenBlocks iter.Seq[[][]byte], fields iter.Seq2[string, uint32]) (iter.Seq[tokensSealBlock], func() token.TableBlock) {
	// We have to build sequence of `token.Block`s and `token.TableBlock` here
	// And thats how it can be depicted:
	//
	//                              f1        f2  f3    f4              f5                    f6
	// fields:       		<---------------><--><---><----><-------------------------><----------------->
	// token.Block's: 		[...........][...........][...........][...........][...........][...........]
	// token.TableEntry's:  {-----------}{--}{--}{---}{----}{-----}{-----------}{----}{-----}{-----------}
	//                          f1        f1  f2   f3   f4     f5        f5       f5     f6        f6
	tableBlock := token.TableBlock{}

	return func(yield func(tokensSealBlock) bool) {
			nextBlock, stop := iter.Pull(makeTokensSealBlocks(tokenBlocks))
			defer stop()

			block, has := nextBlock() // get first block
			if !has {
				bb.lastErr = errors.New("empty token blocks")
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

				// for each field at least one TableEntry must be defined
				for needMoreEntries := true; needMoreEntries; {
					entries = append(entries, calcTokenTableEntry(entryStartTID, fieldMaxTID, blockIndex, block))
					// fieldMaxTID <  block.MaxTID 	: no more entries needed for current field and there is no need to load the next block
					// fieldMaxTID == block.MaxTID	: no more entries needed for current field, but the next block needs to be loaded
					// fieldMaxTID >  block.MaxTID	: more entries are needed for the current field and the next block needs to be loaded
					if fieldMaxTID < block.ext.maxTID {
						break
					}
					needMoreEntries = fieldMaxTID > block.ext.maxTID
					if block, has = nextBlock(); !has {
						if needMoreEntries {
							bb.lastErr = errors.New("fields and tokens not consistent")
							return
						}
						break
					}
					if !yield(block) {
						return
					}
					blockIndex++
					entryStartTID = block.ext.minTID
				}
				entryStartTID = fieldMaxTID + 1
				tableBlock.FieldsTables = append(tableBlock.FieldsTables, token.FieldTable{
					Field:   field,
					Entries: entries,
				})
			}
			if _, has = nextBlock(); has {
				bb.lastErr = errors.New("fields and tokens not consistent")
			}
		},
		func() token.TableBlock { return tableBlock }
}

func calcTokenTableEntry(entryStartTID, lastFieldTID, blockIndex uint32, block tokensSealBlock) *token.TableEntry {
	firstIndex := entryStartTID - block.ext.minTID
	lastIndex := min(lastFieldTID, block.ext.maxTID) - block.ext.minTID
	return &token.TableEntry{
		StartIndex: firstIndex,
		StartTID:   entryStartTID,
		BlockIndex: blockIndex,
		ValCount:   lastIndex - firstIndex + 1,
		MinVal:     string(block.payload.GetToken(int(firstIndex))),
		MaxVal:     string(block.payload.GetToken(int(lastIndex))),
	}
}

// LIDsBlocks generates sequence of LIDs blocks from iterator of token lids providing ordered
// sequence of token lids sorted by TID and LID
func (bb *blocksBuilder) LIDsBlocks(tokenLIDs iter.Seq[[]uint32], blockSize int) iter.Seq[lidsSealBlock] {
	return func(yield func(lidsSealBlock) bool) {
		var (
			tid         uint32
			current     lidsSealBlock
			endOfTID    bool
			isContinued bool
		)

		// init current block
		current.ext.minTID = 1
		current.payload = lids.Block{
			LIDs:    make([]uint32, 0, blockSize),
			Offsets: []uint32{0},
		}

		pushCurrentBlock := func() bool {
			if !endOfTID {
				current.payload.Offsets = append(current.payload.Offsets, uint32(len(current.payload.LIDs)))
			}
			current.payload.IsLastLID = endOfTID  // todo: get rid of IsLastLID
			current.ext.isContinued = isContinued // todo: get rid of isContinued
			isContinued = !endOfTID
			return yield(current)
		}

		for lids := range tokenLIDs {
			tid++
			for _, lid := range lids {
				if len(current.payload.LIDs) == blockSize {
					if !pushCurrentBlock() {
						return
					}
					// reset current block
					current.ext.minTID = tid
					current.payload.LIDs = current.payload.LIDs[:0]
					current.payload.Offsets = current.payload.Offsets[:1]
				}

				endOfTID = false
				current.ext.maxTID = tid
				current.payload.LIDs = append(current.payload.LIDs, lid)
			}
			current.payload.Offsets = append(current.payload.Offsets, uint32(len(current.payload.LIDs)))
			endOfTID = true
		}
		pushCurrentBlock()
	}
}

func makeIDsSealBlocks(idsBlocks iter.Seq2[[]seq.ID, []seq.DocPos]) iter.Seq[idsSealBlock] {
	return func(yield func(idsSealBlock) bool) {
		block := idsSealBlock{}
		for ids, pos := range idsBlocks {
			// init / reset
			block.mids.Values = block.mids.Values[:0]
			block.rids.Values = block.rids.Values[:0]
			block.params.Values = block.params.Values[:0]
			// fill
			for i, id := range ids {
				block.mids.Values = append(block.mids.Values, uint64(id.MID))
				block.rids.Values = append(block.rids.Values, uint64(id.RID))
				block.params.Values = append(block.params.Values, uint64(pos[i]))
			}
			if !yield(block) {
				return
			}
		}
	}
}

func makeTokensSealBlocks(tokenBlocks iter.Seq[[][]byte]) iter.Seq[tokensSealBlock] {
	return func(yield func(tokensSealBlock) bool) {
		var (
			tid   uint32
			block tokensSealBlock
		)
		for tokens := range tokenBlocks {
			// init / reset
			block.ext.minTID = tid + 1
			block.payload.Payload = block.payload.Payload[:0]
			block.payload.Offsets = block.payload.Offsets[:0]
			// fill
			for _, token := range tokens {
				tid++
				block.payload.Offsets = append(block.payload.Offsets, uint32(len(block.payload.Payload)))
				block.payload.Payload = binary.LittleEndian.AppendUint32(block.payload.Payload, uint32(len(token)))
				block.payload.Payload = append(block.payload.Payload, token...)
			}
			block.ext.maxTID = tid
			if !yield(block) {
				return
			}
		}
	}
}
