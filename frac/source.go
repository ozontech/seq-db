package frac

import (
	"iter"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type (
	DocLocation = util.Pair[seq.ID, seq.DocPos]
	TokenLIDs   = util.Pair[[]byte, []uint32]
)

// Source defines the data required to write all index files for a fraction.
type Source interface {
	// Info returns metadata describing this source.
	Info() *Info

	// ID returns an iterator over stored document identifiers paired with
	// their positions, in descending [seq.ID] order.
	IDs() iter.Seq2[DocLocation, error]

	// BlockOffsets returns byte offsets to each document block
	// within this source's `.docs` file.
	BlockOffsets() []uint64

	// TokenTriplet iterates over fields in lexicographic order.
	// For each field, it yields tokens (lexicographically sorted)
	// paired with the local document ID list for that token.
	TokenTriplets() iter.Seq2[string, iter.Seq2[TokenLIDs, error]]
}
