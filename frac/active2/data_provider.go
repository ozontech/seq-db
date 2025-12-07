package active2

import (
	"context"
	"fmt"
	"sort"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/prometheus/client_golang/prometheus"
)

type fetchIndex struct {
	index      *memIndex
	docsReader *storage.DocsReader
}

func (si *fetchIndex) GetBlocksOffsets(blockIndex uint32) uint64 {
	return si.index.blocksOffsets[blockIndex]
}

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

func (si *fetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return si.docsReader.ReadDocs(blockOffset, docOffsets)
}

type searchIndex struct {
	ctx   context.Context
	index *memIndex
}

func (si *searchIndex) GetValByTID(tid uint32) []byte {
	return si.index.tokens[tid]
}

func (si *searchIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	field := parser.GetField(t)
	tp := si.index.getTokenProvider(field)
	tids, err := pattern.Search(si.ctx, t, tp)
	if err != nil {
		return nil, fmt.Errorf("search error: %w field: %s, query: %s", err, field, parser.GetHint(t))
	}
	return tids, nil
}

func (si *searchIndex) GetLIDsFromTIDs(tids []uint32, _ lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		nodes = append(nodes, si.geTidLidsNode(tid, minLID, maxLID, order))
	}
	return nodes
}

func (si *searchIndex) geTidLidsNode(tid, minLID, maxLID uint32, order seq.DocsOrder) node.Node {
	if tid == si.index.allTID {
		return node.NewRange(minLID, maxLID, order.IsReverse())
	}
	tidLIDs := si.index.tokenLIDs[tid]
	return node.NewStatic(narrowDownLIDs(tidLIDs, minLID, maxLID), order.IsReverse())
}

func narrowDownLIDs(tidLIDs []uint32, minLID, maxLID uint32) []uint32 {
	n := len(tidLIDs)
	left := sort.Search(n, func(i int) bool { return tidLIDs[i] >= minLID })
	right := sort.Search(n, func(i int) bool { return tidLIDs[i] > maxLID })
	if left > right {
		return nil
	}
	return tidLIDs[left:right]
}

func (si *searchIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	checkedMID := si.GetMID(lid)
	if checkedMID == id.MID {
		return si.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

func (si *searchIndex) GetMID(lid seq.LID) seq.MID {
	return si.index.ids[lid-1].MID
}

func (si *searchIndex) GetRID(lid seq.LID) seq.RID {
	return si.index.ids[lid-1].RID
}

func (si *searchIndex) Len() int {
	return len(si.index.ids) + 1
}

func getActiveSearchMetric(params processor.SearchParams) *prometheus.HistogramVec {
	if params.HasAgg() {
		return searchAggSec
	}
	if params.HasHist() {
		return searchHstSec
	}
	return searchSimpleSec
}

type tokenProvider struct {
	firstTID uint32
	lastTID  uint32
	tokens   [][]byte
}

func (p *tokenProvider) GetToken(tid uint32) []byte {
	return p.tokens[tid]
}

func (p *tokenProvider) FirstTID() uint32 {
	return p.firstTID
}

func (p *tokenProvider) LastTID() uint32 {
	return p.lastTID
}

func (p *tokenProvider) Ordered() bool {
	return true
}
