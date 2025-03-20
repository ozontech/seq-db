package frac

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type MetaData struct {
	ID seq.ID
	// Size of an uncompressed document in bytes.
	Size   uint32
	Tokens []MetaToken
}

// String used in tests for human-readable output.
func (m MetaData) String() string {
	return fmt.Sprintf("ID: %s, Size: %d, Tokens: %s", m.ID, m.Size, m.Tokens)
}

const metadataMagic = uint16(0x3F7C) // 2 magic bytes of the binary encoded metadata.

func IsItBinaryEncodedMetaData(b []byte) bool {
	if len(b) < 2 {
		return false
	}
	return binary.LittleEndian.Uint16(b) == metadataMagic
}

func (m *MetaData) MarshalBinaryTo(b []byte) []byte {
	// Append "magic bytes" to determine that this is binary encoded metadata.
	b = binary.LittleEndian.AppendUint16(b, metadataMagic)

	// Append current binary version of the metadata.
	const version = 1
	b = binary.LittleEndian.AppendUint16(b, version)

	// Encode seq.ID.
	b = binary.LittleEndian.AppendUint64(b, uint64(m.ID.MID))
	b = binary.LittleEndian.AppendUint64(b, uint64(m.ID.RID))

	// Encode Size.
	b = binary.LittleEndian.AppendUint32(b, m.Size)

	// Encode tokens.
	toksLen := len(m.Tokens)
	b = binary.LittleEndian.AppendUint32(b, uint32(toksLen))
	for i := 0; i < toksLen; i++ {
		b = m.Tokens[i].MarshalBinaryTo(b)
	}

	return b
}

func (m *MetaData) UnmarshalBinary(b []byte) error {
	if !IsItBinaryEncodedMetaData(b) {
		return fmt.Errorf("invalid metadata magic bytes")
	}
	b = b[2:]

	version := binary.LittleEndian.Uint16(b)
	b = b[2:]

	switch version {
	case 1:
		return m.unmarshalVersion1(b)
	default:
		return fmt.Errorf("unimplemented metadata version: %d", version)
	}
}

func (m *MetaData) unmarshalVersion1(b []byte) error {
	// Decode seq.ID.
	m.ID.MID = seq.MID(binary.LittleEndian.Uint64(b))
	b = b[8:]
	m.ID.RID = seq.RID(binary.LittleEndian.Uint64(b))
	b = b[8:]

	// Decode uncompressed document size.
	m.Size = binary.LittleEndian.Uint32(b)
	b = b[4:]

	// Decode tokens length.
	toksLen := binary.LittleEndian.Uint32(b)
	b = b[4:]

	// Decode tokens.
	m.Tokens = make([]MetaToken, toksLen)
	var err error
	for i := uint32(0); i < toksLen; i++ {
		b, err = m.Tokens[i].UnmarshalBinary(b)
		if err != nil {
			return err
		}
	}
	return nil
}

type MetaToken struct {
	Key   []byte
	Value []byte
}

func (m *MetaToken) MarshalBinaryTo(b []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Key)))
	b = append(b, m.Key...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Value)))
	b = append(b, m.Value...)
	return b
}

func (m *MetaToken) UnmarshalBinary(b []byte) ([]byte, error) {
	keyLen := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if int(keyLen) > len(b) {
		return nil, fmt.Errorf("malformed key")
	}
	m.Key = b[:keyLen]
	b = b[keyLen:]

	valueLen := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if int(valueLen) > len(b) {
		return nil, fmt.Errorf("malformed value")
	}
	m.Value = b[:valueLen]
	b = b[valueLen:]
	return b, nil
}

// String used in tests for human-readable output.
func (m MetaToken) String() string {
	return fmt.Sprintf("(%s: %s)", m.Key, m.Value)
}

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
	IDs          []seq.ID     // seq.IDs from bulk request
	tokensInDocs []uint32     // number of tokens in each document in IDs order
	tokensIndex  []int        // positions in TokensValues for each token of each document in IDs order
	Positions    []seq.DocPos // positions in doc-file  of each doc in IDs order
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

func (c *metaDataCollector) AppendMeta(m MetaData) {
	var pos seq.DocPos
	if m.Size == 0 {
		// This is a nested document that must point to the parent.
		pos = c.Positions[len(c.Positions)-1]
	} else {
		pos = seq.PackDocPos(c.blockIndex, c.nextDocOffset)
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
	positions := make([]seq.DocPos, 0, cap(c.Positions))
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

func (c *metaDataCollector) extractTokens(tokens []MetaToken) {
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
		c.Positions = make([]seq.DocPos, 0, size)
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
