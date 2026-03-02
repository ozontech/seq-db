package active

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

// mergeIndexes merges several in-memory indexes into one.
func mergeIndexes(indexes []*memIndex) *memIndex {
	// preallocate memory based on total size
	blocksCount := 0
	dst := newMemIndex()

	for _, idx := range indexes {
		dst.docsSize += idx.docsSize
		dst.docsCount += idx.docsCount
		dst.allTokenLIDsCount += idx.allTokenLIDsCount
		blocksCount += len(idx.blocksOffsets)
	}

	tmp, release := NewResources()
	defer release()

	// preallocate final structures
	dst.ids = dst.res.GetIDs(int(dst.docsCount))[:0]
	dst.positions = dst.res.GetDocPosSlice(int(dst.docsCount))[:0]
	dst.blocksOffsets = dst.res.GetUint64s(blocksCount)[:0]

	// 1. merge block offsets and recalc document positions
	posMap := mergeBlocksOffsets(dst, tmp, indexes)

	// 2. merge documents, get old→new LID mapping
	lidsMap := mergeIDs(dst, tmp, indexes, posMap)

	// 3. merge tokens using new LIDs
	mergeTokens(dst, tmp, indexes, lidsMap)

	return dst
}

// mergeIDs merges documents from all indexes into ordered stream.
// returns mapping oldLID → newLID for each index.
func mergeIDs(
	dst *memIndex,
	tmp *Resources,
	indexes []*memIndex,
	posMap [][]seq.DocPos,
) [][]uint32 {

	// store old→new LID mapping per index
	lidsMap := tmp.GetUint32Slices(len(indexes))
	docStreams := make([]OrderedStream[DocRef], len(indexes))

	for i, idx := range indexes {
		docStreams[i] = &DocStream{
			i:      i,         // index number
			idx:    idx,       // the index itself
			posMap: posMap[i], // recalculated document positions
		}

		// LIDs start from 1, so add dummy element
		lidsMap[i] = tmp.GetUint32s(int(idx.docsCount) + 1)[:1]
	}

	// merge all streams by ID (reverse order)
	mergedDocStream := MergeSortedStreams(
		docStreams,
		func(a, b DocRef) int {
			r := seq.Compare(b.id, a.id)
			if r == 0 {
				r = cmp.Compare(a.i, b.i)
			}
			return r
		},
	)

	var (
		doubles int
		prevRef DocRef
	)
	// process merged stream
	for docRef, has := mergedDocStream.Next(); has; docRef, has = mergedDocStream.Next() {
		if docRef.id == prevRef.id && docRef.i != prevRef.i {
			doubles++
			// map old LID → 0 (will be filtered later)
			lidsMap[docRef.i] = append(lidsMap[docRef.i], 0)
			continue
		}
		prevRef = docRef

		// add to result
		dst.ids = append(dst.ids, docRef.id)
		dst.positions = append(dst.positions, docRef.pos)

		// new LID is position in dst.ids (1-based)
		newLID := uint32(len(dst.ids))
		lidsMap[docRef.i] = append(lidsMap[docRef.i], newLID)
	}

	if doubles > 0 {
		dst.docsCount -= uint32(doubles)
		logger.Warn("doubles in index", zap.Int("count", doubles))
	}

	return lidsMap
}

// mergeTokens merges tokens from all indexes using new LIDs.
func mergeTokens(
	dst *memIndex,
	tmp *Resources,
	indexes []*memIndex,
	lidsMap [][]uint32,
) {
	totalTokens := 0
	tokenStreams := make([]OrderedStream[TokenRef], len(indexes))

	// create token iterators
	for i, idx := range indexes {
		totalTokens += len(idx.tokens)
		tokenStreams[i] = NewTokenStream(idx, lidsMap[i])
	}

	cmpToken := func(a, b TokenRef) int {
		r := bytes.Compare(a.Field(), b.Field())
		if r == 0 {
			return bytes.Compare(a.Value(), b.Value())
		}
		return r
	}

	// unique values statistics
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
		borderNone  = 0b00 // tokensRef[i] same token as previous (but different index)
		borderToken = 0b01 // tokensRef[i] is a new token value
		borderField = 0b10 // tokensRef[i] is a new field
	)

	borders := tmp.GetBytes(totalTokens)[:0]
	tokensRef := make([]TokenRef, 0, totalTokens)

	// merged and sorted token stream
	// first pass: count unique tokens and fields
	mergedTokensStream := MergeSortedStreams(tokenStreams, cmpToken)
	for tokenRef, has := mergedTokensStream.Next(); has; tokenRef, has = mergedTokensStream.Next() {
		var border uint8 = borderNone

		// new token
		if prevToken.payload == nil || cmpToken(prevToken, tokenRef) != 0 {
			uniqTokensCount++
			uniqTokensSize += len(tokenRef.Value())
			border |= borderToken

			// new field
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

	// initialize result structures
	dst.fieldsTokens = make(map[string]tokenRange, uniqFieldsCount)
	dst.fields = dst.res.GetBytesSlices(uniqFieldsCount)[:0]
	dst.tokens = dst.res.GetBytesSlices(uniqTokensCount)[:0]
	dst.tokenLIDs = dst.res.GetUint32Slices(uniqTokensCount)[:0]

	allTokens := dst.res.GetBytes(uniqTokensSize)[:0]
	allFields := dst.res.GetBytes(uniqFieldsSize)[:0]

	// collector for token's document LIDs
	lidsCollector := NewLIDsCollector(
		int(dst.docsCount),
		dst.res.GetUint32s(dst.allTokenLIDsCount)[:0], // all token LIDs
		dst.res.GetUint32s(int(dst.docsCount)),        // LIDs for _all_
		tmp.GetBytes((int(dst.docsCount) + 1)),        // sorting buffer
	)

	// second pass: fill structures
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

		// add document LIDs for this token
		newLIDsMap := tokenRef.lidsMap()
		for _, oldLID := range tokenRef.LIDs() {
			lidsCollector.Add(newLIDsMap[oldLID])
		}
	}

	// final token
	dst.tokenLIDs = append(dst.tokenLIDs, lidsCollector.GetSorted())

	// close last field
	tid := uint32(len(dst.tokens)) - 1
	fieldStr := util.ByteToStringUnsafe(dst.fields[len(dst.fields)-1])
	tr := dst.fieldsTokens[fieldStr]
	tr.count = tid - tr.start + 1
	dst.fieldsTokens[fieldStr] = tr
}

// LIDsCollector collects and sorts document LIDs for a token.
type LIDsCollector struct {
	totalDocs int      // total docs count AFTER deduplication
	lids      []uint32 // overall array
	all       []uint32 // full LID set (1..N)
	bitmap    []uint8  // bitmap
	offset    int
}

// NewLIDsCollector initializes collector.
func NewLIDsCollector(totalDocs int, lids, all []uint32, bitmap []uint8) *LIDsCollector {
	clear(bitmap)
	for i := range all {
		all[i] = uint32(i) + 1
	}
	return &LIDsCollector{
		totalDocs: totalDocs,
		lids:      lids[:0],
		bitmap:    bitmap,
		all:       all,
	}
}

// Add a single LID
func (s *LIDsCollector) Add(lid uint32) {
	if lid == 0 {
		return
	}
	s.lids = append(s.lids, lid)
}

// GetSorted returns sorted LID list using optimal algorithm.
func (s *LIDsCollector) GetSorted() (dst []uint32) {
	n := len(s.lids) - s.offset

	// all documents covered → return all
	if n == s.totalDocs {
		s.lids = s.lids[:s.offset]
		return s.all
	}

	dst = s.lids[s.offset:]
	s.offset = len(s.lids)

	if n == 0 {
		return dst
	}

	if 100*n/s.totalDocs > 30 {
		// dense case: use bitmap
		return s.altSort(dst)
	}

	// sparse case: sort normally
	slices.Sort(dst)
	return dst
}

func (s *LIDsCollector) altSort(vals []uint32) []uint32 {
	for _, v := range vals {
		s.bitmap[v] = 1
	}
	vals = vals[:0]
	for lid, ok := range s.bitmap {
		if ok == 1 {
			s.bitmap[lid] = 0
			vals = append(vals, uint32(lid))
		}
	}
	return vals
}

// mergeBlocksOffsets merges block offsets and recalculates document positions.
func mergeBlocksOffsets(
	dst *memIndex,
	tmp *Resources,
	indexes []*memIndex,
) [][]seq.DocPos {

	var offset uint32
	positions := tmp.GetDocPosSlices(len(indexes))

	for i, index := range indexes {
		// copy block offsets
		dst.blocksOffsets = append(dst.blocksOffsets, index.blocksOffsets...)

		// recalculate positions
		positions[i] = tmp.GetDocPosSlice(len(index.positions))[:0]
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
