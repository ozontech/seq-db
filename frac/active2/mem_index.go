package active2

import (
	"sort"

	"github.com/ozontech/seq-db/seq"
)

type tokenRange struct {
	start uint32
	count uint32
}

type memIndex struct {
	ids           []seq.ID              // IDs ordered DESC
	tokens        [][]byte              // tokens ordered ASC by field:token
	tokenLIDs     [][]uint32            // LIDs list for each token from `tokens`
	fieldsTokens  map[string]tokenRange // tokens locator for each field
	fields        [][]byte              // fields ordered ASC
	blocksOffsets []uint64              // blocks offsets ordered by offset
	idToLID       map[seq.ID]uint32
	positions     []seq.DocPos
	allTID        uint32

	docsSize  uint64
	docsCount uint32
}

func (index *memIndex) getTokenProvider(field string) *tokenProvider {
	if r, ok := index.fieldsTokens[field]; ok {
		return &tokenProvider{
			firstTID: r.start,
			lastTID:  r.start + r.count - 1,
			tokens:   index.tokens,
		}
	}
	// Field is not indexed - return empty token provider
	return &tokenProvider{
		firstTID: 1,
		lastTID:  0, // firstTID > lastTID = no tokens available
		tokens:   index.tokens,
	}
}

func (index *memIndex) IsIntersecting(from, to seq.MID) bool {
	maxMID := index.ids[0].MID
	minMID := index.ids[len(index.ids)-1].MID
	if to < minMID || maxMID < from {
		return false
	}
	return true
}

func (index *memIndex) GetLIDByID(id seq.ID) (uint32, bool) {
	lid, ok := index.idToLID[id]
	return lid, ok

	// alternative
	// todo check to use 1-based lids
	i, ok := sort.Find(len(index.ids), func(i int) int { return seq.Compare(index.ids[i], id) })
	return uint32(i), ok
}
