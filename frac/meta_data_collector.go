package frac

import (
	"math"

	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/ozontech/seq-db/util"
)

// metaDataCollector is a collection of metadata
// metaDataCollector can reuse its fields to process many requests in a row one after another
// metaDataCollector keep track of the size of its fields to avoid memory leak
type metaDataCollector struct {
	// the position in the block of the document following the document whose meta we just appended
	nextDocOffset uint64

	blockIndex uint32

	// stats
	MaxMID, MinMID seq.MID
	DocsCounter    uint32
	SizeCounter    uint64

	// tokens
	tokensBuf       []byte
	TokensValues    [][]byte       // sliced tokensBuf contains unique bucket tokens
	FieldsLengths   []int          // length of field of each token in TokensValues order
	tokensMap       map[string]int // unique tokens with positions in TokensValues as item values
	tokenLIDsPlaces []*TokenLIDs

	// ids
	IDs          []seq.ID // seq.IDs from bulk request
	tokensInDocs []uint32 // number of tokens in each document in IDs order
	tokensIndex  []int    // positions in TokensValues for each token of each document in IDs order
	Positions    []DocPos // positions in doc-file  of each doc in IDs order
	lids         []uint32

	solvers struct {
		ids          util.ReallocSolver
		tokensBuf    util.ReallocSolver
		tokensIndex  util.ReallocSolver
		tokensValues util.ReallocSolver
	}
}

func newMetaDataCollector() *metaDataCollector {
	c := metaDataCollector{
		tokensMap: make(map[string]int),
	}
	c.solvers.ids = util.NewReallocSolver(util.ReallocSolverLabel("MetaDataCollector.IDs"))
	c.solvers.tokensBuf = util.NewReallocSolver(util.ReallocSolverLabel("MetaDataCollector.tokensBuf"))
	c.solvers.tokensIndex = util.NewReallocSolver(util.ReallocSolverLabel("MetaDataCollector.tokensIndex"))
	c.solvers.tokensValues = util.NewReallocSolver(util.ReallocSolverLabel("MetaDataCollector.TokensValues"))
	return &c
}

func (c *metaDataCollector) AppendMeta(m bulk.MetaData) {
	var pos DocPos
	if m.Size == 0 {
		// This is a nested document that must point to the parent.
		pos = c.Positions[len(c.Positions)-1]
	} else {
		pos = PackDocPos(c.blockIndex, c.nextDocOffset)
		const sizeFieldLen = 4 // len of uint32 field storing size of document; see docs/meta.png
		c.nextDocOffset += uint64(m.Size) + sizeFieldLen
	}

	if m.ID.MID < c.MinMID {
		c.MinMID = m.ID.MID
	}
	if m.ID.MID > c.MaxMID {
		c.MaxMID = m.ID.MID
	}

	c.IDs = append(c.IDs, m.ID)
	c.tokensInDocs = append(c.tokensInDocs, uint32(len(m.Tokens)))
	c.Positions = append(c.Positions, pos)

	c.DocsCounter++
	c.SizeCounter += uint64(m.Size)

	c.extractTokens(m.Tokens)
}

func getIndexesOfIntercept(a, b []seq.ID) []int {
	bMap := make(map[seq.ID]struct{}, len(b))
	for _, id := range b {
		bMap[id] = struct{}{}
	}

	res := make([]int, 0, len(a)-len(b))
	for i, id := range a {
		if _, ok := bMap[id]; ok {
			res = append(res, i)
		}
	}
	return res
}

// Filter cleans up metaDataCollector from ids that do not intercept with 'appended'.
func (c *metaDataCollector) Filter(appended []seq.ID) {
	// prepare stats
	c.MaxMID = 0
	c.MinMID = math.MaxUint64
	c.DocsCounter = uint32(len(appended))

	// prepare new slices
	ids := make([]seq.ID, 0, cap(c.IDs))
	positions := make([]DocPos, 0, cap(c.Positions))
	tokensIndex := make([]int, 0, cap(c.tokensIndex))
	tokensInDocs := make([]uint32, 0, cap(c.tokensInDocs))

	// build offsets
	tokensOffset := uint32(0)
	tokensOffsets := make([]uint32, len(c.tokensInDocs))
	for i, v := range c.tokensInDocs {
		tokensOffsets[i] = tokensOffset
		tokensOffset += v
	}

	for _, i := range getIndexesOfIntercept(c.IDs, appended) {
		id := c.IDs[i]
		if id.MID < c.MinMID {
			c.MinMID = id.MID
		}
		if id.MID > c.MaxMID {
			c.MaxMID = id.MID
		}

		ids = append(ids, id)
		positions = append(positions, c.Positions[i])
		tokensInDocs = append(tokensInDocs, c.tokensInDocs[i])
		tokensIndex = append(tokensIndex, c.tokensIndex[tokensOffsets[i]:tokensOffsets[i]+c.tokensInDocs[i]]...)
	}

	c.IDs = ids
	c.Positions = positions
	c.tokensInDocs = tokensInDocs
	c.tokensIndex = tokensIndex
}

func (c *metaDataCollector) extractTokens(tokens []tokenizer.MetaToken) {
	for _, token := range tokens {
		key, value := token.Key, token.Value
		pos := len(c.tokensBuf)
		c.tokensBuf = append(c.tokensBuf, key...)
		c.tokensBuf = append(c.tokensBuf, ':')
		c.tokensBuf = append(c.tokensBuf, value...)

		token := c.tokensBuf[pos:]
		index, ok := c.tokensMap[string(token)]
		if ok {
			c.tokensBuf = c.tokensBuf[:pos] // rollback
		} else {
			index = len(c.TokensValues)
			c.TokensValues = append(c.TokensValues, token)
			c.tokensMap[string(token)] = index
			c.FieldsLengths = append(c.FieldsLengths, len(key))
		}
		c.tokensIndex = append(c.tokensIndex, index)
	}
}

func (c *metaDataCollector) Init(blockIndex uint32) {
	c.nextDocOffset = 0

	c.blockIndex = blockIndex

	c.MaxMID = 0
	c.MinMID = math.MaxUint64

	c.DocsCounter = 0
	c.SizeCounter = 0

	if size, need := c.solvers.ids.ReallocParams(len(c.IDs), cap(c.IDs)); need {
		c.IDs = make([]seq.ID, 0, size)
		c.tokensInDocs = make([]uint32, 0, size)
		c.Positions = make([]DocPos, 0, size)
	} else {
		c.IDs = c.IDs[:0]
		c.tokensInDocs = c.tokensInDocs[:0]
		c.Positions = c.Positions[:0]
	}

	if size, need := c.solvers.tokensBuf.ReallocParams(len(c.tokensBuf), cap(c.tokensBuf)); need {
		c.tokensBuf = make([]byte, 0, size)
	} else {
		c.tokensBuf = c.tokensBuf[:0]
	}

	if size, need := c.solvers.tokensIndex.ReallocParams(len(c.tokensIndex), cap(c.tokensIndex)); need {
		c.lids = make([]uint32, 0, size)
		c.tokensIndex = make([]int, 0, size)
	} else {
		c.lids = c.lids[:0]
		c.tokensIndex = c.tokensIndex[:0]
	}

	for i := range c.TokensValues {
		c.TokensValues[i] = nil // to release rotated tokensBuf
	}

	for i := range c.tokenLIDsPlaces {
		c.tokenLIDsPlaces[i] = nil // to release rotated tokensBuf
	}

	if size, need := c.solvers.tokensValues.ReallocParams(len(c.TokensValues), cap(c.TokensValues)); need {
		estimatedMapSize := len(c.tokensMap) * size / len(c.TokensValues)
		c.tokensMap = make(map[string]int, estimatedMapSize)
		c.FieldsLengths = make([]int, 0, size)
		c.TokensValues = make([][]byte, 0, size)
		c.tokenLIDsPlaces = make([]*TokenLIDs, 0, size)
	} else {
		c.TokensValues = c.TokensValues[:0]
		c.FieldsLengths = c.FieldsLengths[:0]
		c.tokenLIDsPlaces = c.tokenLIDsPlaces[:0]
		clear(c.tokensMap)
	}
}

func (c *metaDataCollector) restoreLIDsOrder(lids []uint32) {
	c.lids = c.lids[:0]
	for i, tokensInDoc := range c.tokensInDocs {
		for j := uint32(0); j < tokensInDoc; j++ {
			c.lids = append(c.lids, lids[i])
		}
	}
}

func (c *metaDataCollector) GroupLIDsByToken(lids []uint32) [][]uint32 {
	cnt := make([]int, len(c.TokensValues))
	for _, j := range c.tokensIndex {
		cnt[j]++
	}

	offset := 0
	lidsGroups := make([][]uint32, len(cnt))
	flatLIDsGroups := make([]uint32, len(c.tokensIndex))
	for i, c := range cnt {
		lidsGroups[i] = flatLIDsGroups[offset:offset]
		offset += c
	}

	c.restoreLIDsOrder(lids)
	for i, j := range c.tokensIndex {
		lidsGroups[j] = append(lidsGroups[j], c.lids[i])
	}
	return lidsGroups
}

func (c *metaDataCollector) PrepareTokenLIDsPlaces() []*TokenLIDs {
	c.tokenLIDsPlaces = util.EnsureSliceSize(c.tokenLIDsPlaces, len(c.TokensValues))
	return c.tokenLIDsPlaces
}
