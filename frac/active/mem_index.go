package active

import (
	"sort"
	"sync"

	"github.com/ozontech/seq-db/seq"
)

// tokenRange describes a range of tokens belonging to a specific field.
type tokenRange struct {
	start uint32 // first TID of the field
	count uint32 // number of tokens in the field
}

// memIndex is an in-memory index of an active segment.
// It is used for searching, mapping tokens to documents, and retrieving document positions.
type memIndex struct {
	// Important:
	//   - index in ids array + 1 = LID (local document id)
	//   - index in positions array + 1 = LID also
	//   - index in tokens array = TID (token id)
	//   - index in fieldsTokens array = TID

	ids           []seq.ID              // list of document IDs sorted in descending order (DESC)
	tokens        [][]byte              // list of all tokens sorted in ascending order (ASC) by key field:token
	tokenLIDs     [][]uint32            // for each TID stores a sorted list of LIDs of documents containing this token
	fieldsTokens  map[string]tokenRange // mapping field → token range (TID) belonging to this field
	fields        [][]byte              // list of all fields sorted in ascending order (ASC)
	blocksOffsets []uint64              // offsets of document blocks in storage, sorted in ascending order
	positions     []seq.DocPos          // position of each document inside a block; index corresponds to LID-1

	hash              uint64
	docsSize          uint64 // total size of documents in bytes
	docsCount         uint32 // number of documents in the index
	allTokenLIDsCount int    // total number of tokenLIDs (for fast calc allocation size in merging)

	wg      sync.WaitGroup // used to wait for background operations to finish before releasing resources
	res     *Resources     // shared resource pool (memory, buffers, etc.)
	release func()         // function to release resources
}

// newMemIndex creates a new in-memory index and initializes resources.
func newMemIndex() *memIndex {
	res, release := NewResources()
	return &memIndex{
		res:     res,
		release: release,
	}
}

// getTokenProvider returns a tokenProvider for the specified field.
// It restricts the TID range to tokens belonging only to this field.
func (idx *memIndex) getTokenProvider(field string) *tokenProvider {
	if r, ok := idx.fieldsTokens[field]; ok {
		return &tokenProvider{
			firstTID: r.start,
			lastTID:  r.start + r.count - 1,
			tokens:   idx.tokens,
		}
	}

	// Field is not indexed — return an empty provider with firstTID > lastTID.
	return &tokenProvider{
		firstTID: 1,
		lastTID:  0,
		tokens:   idx.tokens,
	}
}

// IsIntersecting checks whether the MID range [from, to] intersects
// with the range of documents stored in the index.
func (idx *memIndex) IsIntersecting(from, to seq.MID) bool {
	maxMID := idx.ids[0].MID
	minMID := idx.ids[len(idx.ids)-1].MID

	if to < minMID || maxMID < from {
		return false
	}
	return true
}

// GetLIDByID searches for the local document ID (LID) by global ID (MID + RID).
// Returns the LID (starting from 1) and a flag indicating whether it was found.
func (idx *memIndex) GetLIDByID(id seq.ID) (uint32, bool) {
	i, ok := sort.Find(len(idx.ids), func(i int) int {
		return seq.Compare(idx.ids[i], id)
	})
	return uint32(i + 1), ok
}

// Release frees index resources.
// The call is non-blocking: actual release happens in a separate goroutine
// after all ongoing operations are completed.
func (idx *memIndex) Release() {
	go func() {
		idx.wg.Wait()
		idx.release()
	}()
}
