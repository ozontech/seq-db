package active

import (
	"bytes"
	"slices"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// mergeIndexes merges several in-memory indexes (memIndex)
// into a single resulting index.
func mergeIndexes(indexes []*memIndex) *memIndex {
	// Count the total number of blocks, documents, and tokens to preallocate memory.
	blocksCount := 0
	dst := newMemIndex()

	for _, idx := range indexes {
		dst.docsSize += idx.docsSize
		dst.docsCount += idx.docsCount
		dst.allTokenLIDsCount += idx.allTokenLIDsCount
		blocksCount += len(idx.blocksOffsets)
	}

	// Shared temporary resources for merging
	res, release := NewResources()
	defer release()

	// Preallocate memory for final structures
	dst.ids = dst.res.GetIDs(int(dst.docsCount))[:0]
	dst.positions = dst.res.GetDocPos(int(dst.docsCount))[:0]
	dst.blocksOffsets = dst.res.GetUint64s(blocksCount)[:0]

	// 1. Merge block offsets and recalculate document positions
	posMap := mergeBlocksOffsets(dst, res, indexes)

	// 2. Merge documents (IDs), get old LID → new LID mapping
	lidsMap := mergeIDs(dst, res, indexes, posMap)

	// 3. Merge tokens using the new document LIDs
	mergeTokens(dst, res, indexes, lidsMap)

	return dst
}

// mergeIDs merges documents from all indexes into a single ordered stream.
// Returns a mapping of oldLID → newLID for each input index.
func mergeIDs(
	dst *memIndex,
	res *Resources,
	indexes []*memIndex,
	posMap [][]seq.DocPos,
) [][]uint32 {

	// Store old LID → new LID mapping for each index
	lidsMap := res.GetUint32Slices(len(indexes))

	// Iterators over documents of each index
	docStreams := make([]OrderedStream[DocRef], len(indexes))

	for i, idx := range indexes {
		docStreams[i] = &DocStream{
			i:      i,         // index number
			idx:    idx,       // the index itself
			posMap: posMap[i], // recalculated document positions
		}

		// LIDs start from 1, so add a "dummy" element immediately
		lidsMap[i] = res.GetUint32s(int(idx.docsCount) + 1)[:1]
	}

	// Merge all document streams into one,
	// sorting by ID (in reverse order)
	mergedDocStream := MergeSortedStreams(
		docStreams,
		func(a, b DocRef) int {
			return seq.Compare(b.id, a.id)
		},
	)

	// Iterate over the merged stream
	docRef, has := mergedDocStream.Next()
	for has {
		// Add document to the resulting index
		dst.ids = append(dst.ids, docRef.id)
		dst.positions = append(dst.positions, docRef.pos)

		// New LID is the position in dst.ids (1-based)
		lid := uint32(len(dst.ids))

		// Record oldLID → newLID mapping
		lidsMap[docRef.i] = append(lidsMap[docRef.i], lid)

		docRef, has = mergedDocStream.Next()
	}

	return lidsMap
}

// mergeTokens merges tokens from all indexes,
// reusing the new document LIDs.
func mergeTokens(
	dst *memIndex,
	res *Resources,
	indexes []*memIndex,
	lidsMap [][]uint32,
) {
	totalTokens := 0
	tokenStreams := make([]OrderedStream[TokenRef], len(indexes))

	// create iterators over tokens
	for i, idx := range indexes {
		totalTokens += len(idx.tokens)
		tokenStreams[i] = NewTokenStream(idx, lidsMap[i])
	}

	cmpToken := func(a, b TokenRef) int { // token comparison: first by field, then by value
		r := bytes.Compare(a.Field(), b.Field())
		if r == 0 {
			return bytes.Compare(a.Value(), b.Value())
		}
		return r
	}

	// merged and sorted token stream
	mergedTokenStream := MergeSortedStreams(tokenStreams, cmpToken)

	// statistics for unique values
	uniqTokensSize := 0
	uniqTokensCount := 0
	uniqFieldsSize := 0
	uniqFieldsCount := 0

	var (
		prevField []byte
		prevToken TokenRef
	)

	// borders[i] indicates:
	const (
		borderSame  = 0b00 // tokensRef[i] is the same token as in tokensRef[i-1] (but other index)
		borderToken = 0b01 // tokensRef[i] is new token
		borderField = 0b10 // tokensRef[i] is new token and new field
	)

	borders := res.GetBytes(totalTokens)[:0]
	tokensRef := make([]TokenRef, 0, totalTokens)

	// First pass: count unique tokens and fields
	for tokenRef, has := mergedTokenStream.Next(); has; tokenRef, has = mergedTokenStream.Next() {
		var border uint8 = borderSame

		// New token
		if prevToken.payload == nil || cmpToken(prevToken, tokenRef) != 0 {
			uniqTokensCount++
			uniqTokensSize += len(tokenRef.Value())
			border |= borderToken

			// New field
			field := tokenRef.Field()
			if !bytes.Equal(prevField, field) {
				uniqFieldsCount++
				uniqFieldsSize += len(field)
				border |= borderField
				prevField = field
			}
		}

		borders = append(borders, border)
		tokensRef = append(tokensRef, tokenRef)
		prevToken = tokenRef
	}

	// Initialize resulting index structures
	dst.fieldsTokens = make(map[string]tokenRange, uniqFieldsCount)
	dst.fields = dst.res.GetBytesSlices(uniqFieldsCount)[:0]
	dst.tokens = dst.res.GetBytesSlices(uniqTokensCount)[:0]
	dst.tokenLIDs = dst.res.GetUint32Slices(uniqTokensCount)[:0]

	allTokens := dst.res.GetBytes(uniqTokensSize)[:0]
	allFields := dst.res.GetBytes(uniqFieldsSize)[:0]

	// Collector for document LIDs for each token
	lidsCollector := NewLIDsCollector(
		res.GetUint32s(int(dst.docsCount)),            // temporary buffer
		dst.res.GetUint32s(dst.allTokenLIDsCount)[:0], // all token LIDs
		dst.res.GetUint32s(int(dst.docsCount)),        // LIDs for _all_
		res.GetBytes((int(dst.docsCount) + 1)),        // buffer for sorting
	)

	// Second pass: fill structures
	for i, tokenRef := range tokensRef {
		if borders[i]&borderToken == borderToken { // new token value

			if i > 0 { // finish previous token
				dst.tokenLIDs = append(dst.tokenLIDs, lidsCollector.GetSorted())
			}

			if borders[i]&borderField == borderField { // new field
				tid := uint32(len(dst.tokens))

				if i > 0 { // finish previous field
					fieldStr := util.ByteToStringUnsafe(dst.fields[len(dst.fields)-1])
					tr := dst.fieldsTokens[fieldStr]
					tr.count = tid - tr.start
					dst.fieldsTokens[fieldStr] = tr
				}

				start := len(allFields)
				allFields = append(allFields, tokenRef.Field()...)
				field := allFields[start:]
				dst.fields = append(dst.fields, field)

				fieldStr := util.ByteToStringUnsafe(field)
				dst.fieldsTokens[fieldStr] = tokenRange{start: tid}
			}
			start := len(allTokens)
			allTokens = append(allTokens, tokenRef.Value()...)
			dst.tokens = append(dst.tokens, allTokens[start:])
		}

		// Add document LIDs for the token
		newLIDsMap := tokenRef.lidsMap()
		for _, oldLID := range tokenRef.LIDs() {
			lidsCollector.Add(newLIDsMap[oldLID])
		}
	}

	// Final token
	dst.tokenLIDs = append(dst.tokenLIDs, lidsCollector.GetSorted())

	// Close the last field
	tid := uint32(len(dst.tokens)) - 1
	fieldStr := util.ByteToStringUnsafe(dst.fields[len(dst.fields)-1])
	tr := dst.fieldsTokens[fieldStr]
	tr.count = tid - tr.start + 1
	dst.fieldsTokens[fieldStr] = tr
}

// LIDsCollector collects and efficiently sorts document LIDs for a token.
type LIDsCollector struct {
	tmp  []uint32 // temporary accumulation
	lids []uint32 // overall array
	all  []uint32 // full set of LIDs (1..N)
	buf  []uint8  // bitmap
}

// Initialize collector
func NewLIDsCollector(tmp, lids, all []uint32, buf []uint8) *LIDsCollector {
	clear(buf)
	for i := range all {
		all[i] = uint32(i) + 1
	}
	return &LIDsCollector{
		tmp:  tmp[:0],
		lids: lids[:0],
		all:  all,
		buf:  buf,
	}
}

// Add a single LID
func (s *LIDsCollector) Add(lid uint32) {
	s.tmp = append(s.tmp, lid)
}

// Returns sorted LID list,
// choosing the optimal algorithm depending on density.
func (s *LIDsCollector) GetSorted() (dst []uint32) {
	n := len(s.tmp)

	// If all documents are covered — return all
	if n == len(s.all) {
		s.tmp = s.tmp[:0]
		return s.all
	}

	// If density is high — use bitmap
	if 100*n/len(s.all) > 50 {
		for _, v := range s.tmp {
			s.buf[v] = 1
		}
		start := len(s.lids)
		for lid, ok := range s.buf {
			if ok == 1 {
				s.buf[lid] = 0
				s.lids = append(s.lids, uint32(lid))
			}
		}
		s.tmp = s.tmp[:0]
		return s.lids[start:]
	}

	// Otherwise, normal sorting
	if n > 1 {
		slices.Sort(s.tmp)
	}
	start := len(s.lids)
	s.lids = append(s.lids, s.tmp...)
	s.tmp = s.tmp[:0]
	return s.lids[start:]
}

// mergeBlocksOffsets merges block offsets
// and recalculates document positions considering the offset.
func mergeBlocksOffsets(
	dst *memIndex,
	res *Resources,
	indexes []*memIndex,
) [][]seq.DocPos {

	var offset uint32
	positions := res.GetDocPosSlices(len(indexes))

	for i, index := range indexes {
		// Copy block offsets
		dst.blocksOffsets = append(dst.blocksOffsets, index.blocksOffsets...)

		// Recalculate document positions
		positions[i] = res.GetDocPos(len(index.positions))[:0]
		for _, p := range index.positions {
			oldIdx, docOffset := p.Unpack()
			positions[i] = append(
				positions[i],
				seq.PackDocPos(oldIdx+offset, docOffset),
			)
		}

		offset += uint32(len(index.blocksOffsets))
	}

	return positions
}
