package active

import (
	"context"
	"fmt"
	"sort"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

// fetchIndex is used during the fetch phase:
// reading data and document positions.
type fetchIndex struct {
	index      *memIndex
	docsReader *storage.DocsReader
}

// GetBlocksOffsets returns the offset of a block by its index.
func (si *fetchIndex) GetBlocksOffsets(blockIndex uint32) uint64 {
	return si.index.blocksOffsets[blockIndex]
}

// GetDocPos returns document positions for the given IDs.
// If a document is not found, DocPosNotFound is returned.
func (si *fetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	docsPos := make([]seq.DocPos, len(ids))
	for i, id := range ids {
		if lid, ok := si.index.GetLIDByID(id); ok {
			docsPos[i] = si.index.positions[lid-1]
			continue
		}
		docsPos[i] = seq.DocPosNotFound
	}
	return docsPos
}

// ReadDocs reads documents from storage
// using the block offset and document offsets inside the block.
func (si *fetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return si.docsReader.ReadDocs(blockOffset, docOffsets)
}

// searchIndex is used during the search phase:
// matching tokens, documents, and query conditions.
type searchIndex struct {
	ctx   context.Context
	index *memIndex
}

// GetValByTID returns the token value by its TID.
func (si *searchIndex) GetValByTID(tid uint32) []byte {
	return si.index.tokens[tid]
}

// GetTIDsByTokenExpr finds TIDs by a token expression from the query.
func (si *searchIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	field := parser.GetField(t)
	tp := si.index.getTokenProvider(field)

	tids, err := pattern.Search(si.ctx, t, tp)
	if err != nil {
		return nil, fmt.Errorf("search error: %w field: %s, query: %s", err, field, parser.GetHint(t))
	}
	return tids, nil
}

// GetLIDsFromTIDs converts a list of TIDs into a list of nodes (Node),
// each representing a set of local document identifiers (LIDs)
// that satisfy the token.
func (si *searchIndex) GetLIDsFromTIDs(tids []uint32, _ lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		nodes = append(nodes, si.getTIDLIDsNode(tid, minLID, maxLID, order))
	}
	return nodes
}

// getTIDLIDsNode creates a node.Node for a single TID.
func (si *searchIndex) getTIDLIDsNode(tid, minLID, maxLID uint32, order seq.DocsOrder) node.Node {
	tidLIDs := si.index.tokenLIDs[tid]
	if len(tidLIDs) == 0 { // empty list means ALL documents
		return node.NewRange(minLID, maxLID, order.IsReverse())
	}
	// Regular token — static list of LIDs
	return node.NewStatic(narrowDownLIDs(tidLIDs, minLID, maxLID), order.IsReverse())
}

// narrowDownLIDs restricts a sorted list of LIDs to the range [minLID, maxLID].
func narrowDownLIDs(tidLIDs []uint32, minLID, maxLID uint32) []uint32 {
	n := len(tidLIDs)

	left := sort.Search(n, func(i int) bool {
		return tidLIDs[i] >= minLID
	})
	right := sort.Search(n, func(i int) bool {
		return tidLIDs[i] > maxLID
	})

	if left > right {
		return nil
	}
	return tidLIDs[left:right]
}

// LessOrEqual compares a document by LID with the given ID.
func (si *searchIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	checkedMID := si.GetMID(lid)
	if checkedMID == id.MID {
		return si.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

// GetMID returns the document MID by LID.
func (si *searchIndex) GetMID(lid seq.LID) seq.MID {
	return si.index.ids[lid-1].MID
}

// GetRID returns the document RID by LID.
func (si *searchIndex) GetRID(lid seq.LID) seq.RID {
	return si.index.ids[lid-1].RID
}

// Len returns the number of documents + 1 (LID starts from 1).
func (si *searchIndex) Len() int {
	return len(si.index.ids) + 1
}

// tokenProvider is an adapter for pattern.Search.
// It provides access to tokens in the specified TID range.
type tokenProvider struct {
	firstTID uint32
	lastTID  uint32
	tokens   [][]byte
}

// GetToken returns a token by TID.
func (p *tokenProvider) GetToken(tid uint32) []byte {
	return p.tokens[tid]
}

// FirstTID returns the minimum TID.
func (p *tokenProvider) FirstTID() uint32 {
	return p.firstTID
}

// LastTID returns the maximum TID.
func (p *tokenProvider) LastTID() uint32 {
	return p.lastTID
}

// Ordered reports that tokens are sorted.
func (p *tokenProvider) Ordered() bool {
	return true
}
