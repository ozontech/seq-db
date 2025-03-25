package active2

import (
	"context"
	"fmt"
	"sort"

	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type searchIndex struct {
	ctx   context.Context
	index *Index
}

func (si *searchIndex) GetValByTID(tid uint32) []byte {
	return si.index.tokens[tid]
}

func (si *searchIndex) GetTIDsByTokenExpr(token parser.Token) ([]uint32, error) {
	field := parser.GetField(token)
	data := si.index.fieldsTokens[field]
	f := si.index.getTokenFetcher(data)
	s := pattern.NewSearcher(token, f, data.count)

	tids := []uint32{}
	for i := s.Begin(); i <= s.End(); i++ {
		if util.IsCancelled(si.ctx) {
			return nil, fmt.Errorf(
				"search cancelled when matching tokens: reason=%w field=%s, query=%s",
				si.ctx.Err(),
				field,
				parser.GetHint(token),
			)
		}
		if s.Check(f.FetchToken(i)) {
			tids = append(tids, uint32(data.start+i))
		}
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
	right := sort.Search(n, func(i int) bool { return tidLIDs[i] > maxLID }) - 1
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
	return len(si.index.ids) + 1 // ??
}
