package active2

import "github.com/ozontech/seq-db/seq"

// OrderedStream - interface for iterators with ordered elements
type OrderedStream[T any] interface {
	Next() (T, bool) // Returns the next element and a flag indicating if an element exists
}

// MergeSortedStreams - performs K-way merging of sorted iterators (merge sort at iterator level)
// Uses a "divide and conquer" strategy for efficient merging
func MergeSortedStreams[T any](src []OrderedStream[T], cmp func(T, T) int) OrderedStream[T] {
	n := len(src)
	// Base case of recursion: if there's only one iterator
	if n == 1 {
		return src[0]
	}
	// Recursively split the iterator array in half
	h := n / 2
	src1 := MergeSortedStreams(src[:h], cmp) // Left half
	src2 := MergeSortedStreams(src[h:], cmp) // Right half
	// Merge the two sorted halves
	return NewTwoWayMergeStream(src1, src2, cmp)
}

// TwoWayMergeStream - implementation of an iterator for merging two sorted streams
type TwoWayMergeStream[T any] struct {
	v1, v2     T                // Current values from each source
	has1, has2 bool             // Flags indicating the presence of current values
	src1, src2 OrderedStream[T] // Source iterators
	cmp        func(T, T) int   // Comparison function for sorting
}

// NewTwoWayMergeStream - constructor for MergeIterator
// Initializes the iterator and prefetches the first values from both sources
func NewTwoWayMergeStream[T any](src1, src2 OrderedStream[T], cmp func(T, T) int) *TwoWayMergeStream[T] {
	r := TwoWayMergeStream[T]{
		src1: src1,
		src2: src2,
		cmp:  cmp,
	}
	// Prefetch the first values to enable comparison
	r.v1, r.has1 = r.src1.Next()
	r.v2, r.has2 = r.src2.Next()
	return &r
}

// Next - returns the next element when merging two sorted streams
// Algorithm is similar to merging in mergesort, but works with streams
func (s *TwoWayMergeStream[T]) Next() (v T, has bool) {
	if s.has1 && s.has2 {
		if s.cmp(s.v1, s.v2) < 0 {
			v = s.v1
			s.v1, s.has1 = s.src1.Next()
		} else {
			v = s.v2
			s.v2, s.has2 = s.src2.Next()
		}
		return v, true
	}
	if s.has1 {
		v = s.v1
		s.v1, s.has1 = s.src1.Next()
		return v, true
	}
	if s.has2 {
		v = s.v2
		s.v2, s.has2 = s.src2.Next()
		return v, true
	}
	return v, false
}

// DocRef - item of the document identifier iterator
// Contains information about the document's position in the index
type DocRef struct {
	i   int        // Stream index (for identifying the source)
	id  seq.ID     // Document identifier
	pos seq.DocPos // Document position
}

// DocStream - iterator over the array of document identifiers in memIndex
type DocStream struct {
	i      int          // Stream index (source identifier)
	offset int          // Current position in the ids array
	idx    *memIndex    // Reference to the in-memory index
	posMap []seq.DocPos // Map of document positions
}

// Next - returns the next document ID from memIndex
func (it *DocStream) Next() (v DocRef, has bool) {
	// Check if we haven't exceeded the bounds of the identifiers array
	if it.offset < len(it.idx.ids) {
		v.i = it.i
		v.id = it.idx.ids[it.offset]
		v.pos = it.posMap[it.offset]
		has = true
		it.offset++ // Move pointer for the next call
	}
	return v, has
}

// TokenContext - shared data for the token iterator
// Contains a reference to the index and local identifier mapping
type TokenContext struct {
	idx     *memIndex // In-memory index
	lidsMap []uint32  // Local identifiers map
}

// TokenRef - item of the token iterator
// Represents a single token with metadata
type TokenRef struct {
	tid     uint32        // Token identifier
	fid     uint32        // Field identifier
	payload *TokenContext // Shared data
}

// Field - returns the field name by its identifier
func (i *TokenRef) Field() []byte {
	return i.payload.idx.fields[i.fid]
}

// Value - returns the token value by its identifier
func (i *TokenRef) Value() []byte {
	return i.payload.idx.tokens[i.tid]
}

// LIDs - returns the list of local identifiers for the token
func (i *TokenRef) LIDs() []uint32 {
	return i.payload.idx.tokenLIDs[i.tid]
}

// lidsMap - returns the local identifiers map
func (i *TokenRef) lidsMap() []uint32 {
	return i.payload.lidsMap
}

// TokenStream - iterator over tokens in the index
// Iterates through tokens grouped by fields
type TokenStream struct {
	tid          uint32       // Current token identifier
	fid          uint32       // Current field identifier
	fieldLastTID uint32       // Last TID of the current field
	payload      TokenContext // Iterator shared data
}

// NewTokenStream - constructor for TokenIterator
// Initializes the iterator with starting values
func NewTokenStream(idx *memIndex, lidsMap []uint32) *TokenStream {
	return &TokenStream{
		// Calculate the last TID for the first field
		fieldLastTID: idx.fieldsTokens[string(idx.fields[0])].count - 1,
		payload: TokenContext{
			idx:     idx,
			lidsMap: lidsMap,
		},
	}
}

// Next - returns the next token from the index
// Sequentially iterates through tokens, switching between fields
func (it *TokenStream) Next() (v TokenRef, has bool) {
	// Check if we haven't exceeded the bounds of the tokens array
	if int(it.tid) < len(it.payload.idx.tokens) {
		v.tid = uint32(it.tid)
		v.fid = uint32(it.fid)
		v.payload = &it.payload
		has = true
		it.tid++ // Move to the next token

		// Check if we've reached the end of the current field
		if it.tid > it.fieldLastTID {
			it.fid++ // Move to the next field
			// If there's a next field, update the boundary for the new field
			if int(it.fid) < len(it.payload.idx.fields) {
				// Sum the token counts of fields to get the new boundary
				it.fieldLastTID += it.payload.idx.fieldsTokens[string(it.payload.idx.fields[it.fid])].count
			}
		}
	}
	return v, has
}
