package frac

import (
	"bytes"
	"sort"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type DiskBlocksProducer struct {
	sortedTids map[string][]uint32
	fields     []string
}

func NewDiskBlocksProducer() *DiskBlocksProducer {
	return &DiskBlocksProducer{
		sortedTids: make(map[string][]uint32),
	}
}

func (g *DiskBlocksProducer) getInfoBlock(info *Info) *DiskInfoBlock {
	return &DiskInfoBlock{info: info}
}

func (g *DiskBlocksProducer) getPositionBlock(idsLen uint32, blocks []uint64) *DiskPositionsBlock {
	return &DiskPositionsBlock{
		totalIDs: idsLen,
		blocks:   blocks,
	}
}

func (g *DiskBlocksProducer) getTokenTableBlocksGenerator(tokenList *TokenList, tokenTable token.Table) func(func(*DiskTokenTableBlock) error) error {
	return func(push func(*DiskTokenTableBlock) error) error {
		for _, field := range g.getFracSortedFields(tokenList) {
			if fieldData, ok := tokenTable[field]; ok {
				block := DiskTokenTableBlock{
					field:   field,
					entries: fieldData.Entries,
				}
				if err := push(&block); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func (g *DiskBlocksProducer) getIDsBlocksGenerator(sortedSeqIDs []seq.ID, docsPositions *DocsPositions, size int) func(func(*DiskIDsBlock) error) error {
	return func(push func(*DiskIDsBlock) error) error {
		pos := make([]uint64, 0, size)

		for len(sortedSeqIDs) > 0 {
			right := min(size, len(sortedSeqIDs))
			ids := sortedSeqIDs[:right]
			sortedSeqIDs = sortedSeqIDs[right:]
			pos = g.fillPos(docsPositions, ids, pos)
			block := DiskIDsBlock{
				ids: ids,
				pos: pos,
			}
			if err := push(&block); err != nil {
				return nil
			}
		}

		return nil
	}
}

func (g *DiskBlocksProducer) fillPos(positions *DocsPositions, ids []seq.ID, pos []uint64) []uint64 {
	pos = pos[:len(ids)] // we assume that pos has enough capacity
	for i, id := range ids {
		pos[i] = uint64(positions.Get(id))
	}
	return pos
}

func (g *DiskBlocksProducer) getFracSortedFields(tokenList *TokenList) []string {
	if g.fields == nil {
		g.fields = make([]string, 0, len(tokenList.FieldTIDs))
		for field := range tokenList.FieldTIDs {
			g.fields = append(g.fields, field)
		}
		sort.Strings(g.fields)
	}
	return g.fields
}

type valSort struct {
	val    []uint32
	lessFn func(i, j int) bool
}

func (p *valSort) Len() int           { return len(p.val) }
func (p *valSort) Less(i, j int) bool { return p.lessFn(i, j) }
func (p *valSort) Swap(i, j int)      { p.val[i], p.val[j] = p.val[j], p.val[i] }

func (g *DiskBlocksProducer) getTIDsSortedByToken(tokenList *TokenList, field string) []uint32 {
	if tids, ok := g.sortedTids[field]; ok {
		return tids
	}

	srcTIDs := tokenList.FieldTIDs[field]
	tids := append(make([]uint32, 0, len(srcTIDs)), srcTIDs...)

	sort.Sort(
		&valSort{
			val: tids,
			lessFn: func(i int, j int) bool {
				a := tokenList.tidToVal[tids[i]]
				b := tokenList.tidToVal[tids[j]]
				return bytes.Compare(a, b) < 0
			},
		},
	)

	g.sortedTids[field] = tids
	return tids
}

func (g *DiskBlocksProducer) getTokensBlocksGenerator(tokenList *TokenList) func(func(*DiskTokensBlock) error) error {
	return func(push func(*DiskTokensBlock) error) error {
		var cur uint32 = 1
		var tokens [][]byte

		fieldSizes := tokenList.GetFieldSizes()

		for _, field := range g.getFracSortedFields(tokenList) {
			first := true
			fieldSize := int(fieldSizes[field])
			blocksCount := fieldSize/consts.RegularBlockSize + 1

			tids := g.getTIDsSortedByToken(tokenList, field)
			blockSize := len(tids) / blocksCount

			for len(tids) > 0 {
				right := min(blockSize, len(tids))
				tokens = g.fillTokens(tokenList, tids[:right], tokens)
				tids = tids[right:]

				block := DiskTokensBlock{
					field:            field,
					isStartOfField:   first,
					totalSizeOfField: fieldSize,
					startTID:         cur,
					tokens:           tokens,
				}

				if err := push(&block); err != nil {
					return err
				}

				first = false
				cur += uint32(right)
			}
		}
		return nil
	}
}

func (g *DiskBlocksProducer) fillTokens(tokenList *TokenList, tids []uint32, tokens [][]byte) [][]byte {
	tokens = util.EnsureSliceSize(tokens, len(tids))
	for i, tid := range tids {
		tokens[i] = tokenList.tidToVal[tid]
	}
	return tokens
}

func (g *DiskBlocksProducer) getLIDsBlockGenerator(tokenList *TokenList, oldToNewLIDsIndex []uint32, mids, rids *UInt64s, maxBlockSize int) func(func(*lidsBlock) error) error {
	var maxTID, lastMaxTID uint32

	isContinued := false
	offsets := []uint32{0} // first offset is always zero
	blockLIDs := make([]uint32, 0, maxBlockSize)

	newBlockFn := func(isLastLID bool) *lidsBlock {
		block := &lidsBlock{
			// for continued block we will have minTID > maxTID
			// this is not a bug, everything is according to plan for now
			// TODO: But in future we want to get rid of this
			minTID:      lastMaxTID + 1,
			maxTID:      maxTID,
			isContinued: isContinued,
			payload: lids.Block{
				LIDs:      reassignLIDs(blockLIDs, oldToNewLIDsIndex),
				Offsets:   offsets,
				IsLastLID: isLastLID,
			},
		}
		lastMaxTID = maxTID
		isContinued = !isLastLID

		// reset for reuse
		offsets = offsets[:1] // keep the first offset, which is always zero
		blockLIDs = blockLIDs[:0]

		return block
	}

	return func(push func(*lidsBlock) error) error {
		for _, field := range g.getFracSortedFields(tokenList) {
			for _, tid := range g.getTIDsSortedByToken(tokenList, field) {
				maxTID++
				tokenLIDs := tokenList.Provide(tid).GetLIDs(mids, rids)

				for len(tokenLIDs) > 0 {
					right := min(maxBlockSize-len(blockLIDs), len(tokenLIDs))
					blockLIDs = append(blockLIDs, tokenLIDs[:right]...)
					offsets = append(offsets, uint32(len(blockLIDs)))
					tokenLIDs = tokenLIDs[right:]

					if len(blockLIDs) == maxBlockSize {
						if err := push(newBlockFn(len(tokenLIDs) == 0)); err != nil {
							return nil
						}
					}
				}
			}

			if len(blockLIDs) > 0 {
				if err := push(newBlockFn(true)); err != nil {
					return nil
				}
			}
		}
		return nil
	}
}

func reassignLIDs(lIDs, oldToNewLIDsIndex []uint32) []uint32 {
	for i, lid := range lIDs {
		lIDs[i] = oldToNewLIDsIndex[lid]
	}
	return lIDs
}

func sortSeqIDs(f *Active, mids, rids []uint64) ([]seq.ID, []uint32) {
	seqIDs := make([]seq.ID, len(mids))
	index := make([]uint32, len(mids))

	// some stub value in zero position
	seqIDs[0] = seq.ID{
		MID: seq.MID(mids[0]),
		RID: seq.RID(rids[0]),
	}

	subSeqIDs := seqIDs[1:]

	for i, lid := range f.GetAllDocuments() {
		subSeqIDs[i] = seq.ID{
			MID: seq.MID(mids[lid]),
			RID: seq.RID(rids[lid]),
		}
		index[lid] = uint32(i + 1)
	}

	return seqIDs, index
}
