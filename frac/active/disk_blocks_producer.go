package active

import (
	"bytes"
	"sort"
	"sync"

	"github.com/gogo/protobuf/sortkeys"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type DiskBlocksProducer struct {
	frac *Active

	tidsMu     sync.Mutex
	sortedTids map[string][]uint32
	fields     []string

	sortedSeqIDs      []seq.ID
	oldToNewLIDsIndex []uint32
}

func NewDiskBlocksProducer(frac *Active) *DiskBlocksProducer {
	return &DiskBlocksProducer{
		frac:       frac,
		sortedTids: make(map[string][]uint32),
	}
}

func (g *DiskBlocksProducer) getInfoBlock() *sealed.DiskInfoBlock {
	g.frac.BuildInfoDistribution(g.getSortedSeqIDs())
	return &sealed.DiskInfoBlock{Info: g.frac.Info()}
}

func (g *DiskBlocksProducer) getPositionBlock() *sealed.DiskPositionsBlock {
	return &sealed.DiskPositionsBlock{
		TotalIDs: g.frac.mids.Len(),
		Blocks:   g.frac.docBlocks.GetVals(),
	}
}

func (g *DiskBlocksProducer) getTokenTableBlocksGenerator(tokenTable token.Table) func(func(*sealed.DiskTokenTableBlock) error) error {
	return func(push func(*sealed.DiskTokenTableBlock) error) error {
		for _, field := range g.getFracSortedFields() {
			if fieldData, ok := tokenTable[field]; ok {
				block := sealed.DiskTokenTableBlock{
					Field:   field,
					Entries: fieldData.Entries,
				}
				if err := push(&block); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func (g *DiskBlocksProducer) getIDsBlocksGenerator(size int) func(func(*sealed.DiskIDsBlock) error) error {
	return func(push func(*sealed.DiskIDsBlock) error) error {
		pos := make([]uint64, 0, size)
		sortedSeqIDs := g.getSortedSeqIDs()

		for len(sortedSeqIDs) > 0 {
			right := min(size, len(sortedSeqIDs))
			ids := sortedSeqIDs[:right]
			sortedSeqIDs = sortedSeqIDs[right:]
			pos = g.fillPos(ids, pos)
			block := sealed.DiskIDsBlock{
				IDs: ids,
				Pos: pos,
			}
			if err := push(&block); err != nil {
				return nil
			}
		}

		return nil
	}
}

func (g *DiskBlocksProducer) fillPos(ids []seq.ID, pos []uint64) []uint64 {
	pos = pos[:len(ids)] // we assume that pos has enough capacity
	for i, id := range ids {
		pos[i] = uint64(g.frac.docsPositions.Get(id))
	}
	return pos
}

func (g *DiskBlocksProducer) getFracSortedFields() []string {
	if g.fields == nil {
		g.fields = make([]string, 0, len(g.frac.tokenList.FieldTIDs))
		for field := range g.frac.tokenList.FieldTIDs {
			g.fields = append(g.fields, field)
		}
		sortkeys.Strings(g.fields)
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

func (g *DiskBlocksProducer) getTIDsSortedByToken(field string) []uint32 {
	g.tidsMu.Lock()
	defer g.tidsMu.Unlock()

	if tids, ok := g.sortedTids[field]; ok {
		return tids
	}

	srcTIDs := g.frac.tokenList.FieldTIDs[field]
	tids := append(make([]uint32, 0, len(srcTIDs)), srcTIDs...)

	sort.Sort(
		&valSort{
			val: tids,
			lessFn: func(i int, j int) bool {
				a := g.frac.tokenList.tidToVal[tids[i]]
				b := g.frac.tokenList.tidToVal[tids[j]]
				return bytes.Compare(a, b) < 0
			},
		},
	)

	g.sortedTids[field] = tids
	return tids
}

func (g *DiskBlocksProducer) getTokensBlocksGenerator() func(func(*sealed.DiskTokensBlock) error) error {
	return func(push func(*sealed.DiskTokensBlock) error) error {
		var cur uint32 = 1
		var tokens [][]byte

		fieldSizes := g.frac.tokenList.GetFieldSizes()

		for _, field := range g.getFracSortedFields() {
			first := true
			fieldSize := int(fieldSizes[field])
			blocksCount := fieldSize/consts.RegularBlockSize + 1

			tids := g.getTIDsSortedByToken(field)
			blockSize := len(tids) / blocksCount

			for len(tids) > 0 {
				right := min(blockSize, len(tids))
				tokens = g.fillTokens(tids[:right], tokens)
				tids = tids[right:]

				block := sealed.DiskTokensBlock{
					Field:            field,
					IsStartOfField:   first,
					TotalSizeOfField: fieldSize,
					StartTID:         cur,
					Tokens:           tokens,
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

func (g *DiskBlocksProducer) fillTokens(tids []uint32, tokens [][]byte) [][]byte {
	tokens = util.EnsureSliceSize(tokens, len(tids))
	for i, tid := range tids {
		tokens[i] = g.frac.tokenList.tidToVal[tid]
	}
	return tokens
}

func (g *DiskBlocksProducer) getLIDsBlockGenerator(maxBlockSize int) func(func(*lids.Block) error) error {
	var maxTID, lastMaxTID uint32

	isContinued := false
	offsets := []uint32{0} // first offset is always zero
	blockLIDs := make([]uint32, 0, maxBlockSize)
	oldToNewLIDsIndex := g.getOldToNewLIDsIndex()

	newBlockFn := func(isLastLID bool) *lids.Block {
		block := &lids.Block{
			// for continued block we will have minTID > maxTID
			// this is not a bug, everything is according to plan for now
			// TODO: But in future we want to get rid of this
			MinTID:      lastMaxTID + 1,
			MaxTID:      maxTID,
			IsContinued: isContinued,
			Chunks: lids.Chunks{
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

	return func(push func(*lids.Block) error) error {
		for _, field := range g.getFracSortedFields() {
			for _, tid := range g.getTIDsSortedByToken(field) {
				maxTID++
				tokenLIDs := g.frac.tokenList.Provide(tid).GetLIDs(g.frac.mids, g.frac.rids)

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

func (g *DiskBlocksProducer) getSortedSeqIDs() []seq.ID {
	if g.sortedSeqIDs == nil {
		g.sortedSeqIDs, g.oldToNewLIDsIndex = g.sortSeqIDs()
	}
	return g.sortedSeqIDs
}

func (g *DiskBlocksProducer) getOldToNewLIDsIndex() []uint32 {
	if g.sortedSeqIDs == nil {
		g.sortedSeqIDs, g.oldToNewLIDsIndex = g.sortSeqIDs()
	}
	return g.oldToNewLIDsIndex
}

func (g *DiskBlocksProducer) sortSeqIDs() ([]seq.ID, []uint32) {
	mids := g.frac.mids.GetVals()
	rids := g.frac.rids.GetVals()

	seqIDs := make([]seq.ID, len(mids))
	index := make([]uint32, len(mids))

	// some stub value in zero position
	seqIDs[0] = seq.ID{
		MID: seq.MID(mids[0]),
		RID: seq.RID(rids[0]),
	}

	subSeqIDs := seqIDs[1:]

	for i, lid := range g.frac.GetAllDocuments() {
		subSeqIDs[i] = seq.ID{
			MID: seq.MID(mids[lid]),
			RID: seq.RID(rids[lid]),
		}
		index[lid] = uint32(i + 1)
	}

	return seqIDs, index
}
