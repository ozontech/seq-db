package active2

import (
	"sort"
	"sync"

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
	positions     []seq.DocPos
	allTID        uint32

	docsSize          uint64
	docsCount         uint32
	allTokenLIDsCount int

	wg      sync.WaitGroup
	res     *Resources
	release func()
}

func newMemIndex() *memIndex {
	res, release := AcquireResources()
	return &memIndex{
		res:     res,
		release: release,
	}
}

func (idx *memIndex) getTokenProvider(field string) *tokenProvider {
	if r, ok := idx.fieldsTokens[field]; ok {
		return &tokenProvider{
			firstTID: r.start,
			lastTID:  r.start + r.count - 1,
			tokens:   idx.tokens,
		}
	}
	// Field is not indexed - return empty token provider
	return &tokenProvider{
		firstTID: 1,
		lastTID:  0, // firstTID > lastTID = no tokens available
		tokens:   idx.tokens,
	}
}

func (idx *memIndex) IsIntersecting(from, to seq.MID) bool {
	maxMID := idx.ids[0].MID
	minMID := idx.ids[len(idx.ids)-1].MID
	if to < minMID || maxMID < from {
		return false
	}
	return true
}

func (idx *memIndex) GetLIDByID(id seq.ID) (uint32, bool) {
	i, ok := sort.Find(len(idx.ids), func(i int) int { return seq.Compare(idx.ids[i], id) })
	return uint32(i + 1), ok
}

func (idx *memIndex) Release() {
	idx.wg.Wait()
	idx.release()
}
