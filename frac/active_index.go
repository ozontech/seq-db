package frac

import (
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type ActiveIndexProvider struct {
	f        *Active
	sc       *SearchCell
	idsIndex *ActiveIDsIndex
}

func (ip *ActiveIndexProvider) Indexes() []Index {
	return []Index{&ActiveIndex{ip: ip}}
}

func (ip *ActiveIndexProvider) getIDsIndex() *ActiveIDsIndex {
	if ip.idsIndex == nil {
		ip.idsIndex = ip.createIDsIndex()
	}
	return ip.idsIndex
}

func (ip *ActiveIndexProvider) createIDsIndex() *ActiveIDsIndex {
	mapping := ip.f.GetAllDocuments() // creation order is matter
	mids := ip.f.MIDs.GetVals()       // mids and rids should be created after mapping to ensure that
	rids := ip.f.RIDs.GetVals()       // they contain all the ids that mapping contains.

	inverser := newInverser(mapping, len(mids))

	return &ActiveIDsIndex{
		inverser: inverser,
		mids:     mids,
		rids:     rids,
	}
}

type ActiveIndex struct {
	ip *ActiveIndexProvider
}

func (index *ActiveIndex) IsIntersecting(from, to seq.MID) bool {
	return index.ip.f.IsIntersecting(from, to)
}
func (index *ActiveIndex) Contains(mid seq.MID) bool {
	return index.ip.f.Contains(mid)
}

func (index *ActiveIndex) IDsIndex() IDsIndex {
	return index.ip.getIDsIndex()
}

func (index *ActiveIndex) TokenIndex() TokenIndex {
	return &ActiveTokenIndex{ip: index.ip}
}

func (index *ActiveIndex) DocsIndex() DocsIndex {
	return &ActiveDocsIndex{
		blocksOffsets: index.ip.f.DocBlocks.GetVals(),
		docsPositions: index.ip.f.DocsPositions,
		docsReader:    index.ip.f.docsReader,
	}
}

type ActiveTokenIndex struct {
	ip *ActiveIndexProvider
}

func (ti *ActiveTokenIndex) GetValByTID(tid uint32) []byte {
	return ti.ip.f.TokenList.GetValByTID(tid)
}

func (ti *ActiveTokenIndex) GetTIDsByTokenExpr(token parser.Token) ([]uint32, error) {
	return ti.ip.f.TokenList.FindPattern(ti.ip.sc.Context, token, nil)
}

func (ti *ActiveTokenIndex) GetLIDsFromTIDs(tids []uint32, _ lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	f := ti.ip.f
	inverser := ti.ip.getIDsIndex().inverser

	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		tlids := f.TokenList.Provide(tid)

		unmapped := tlids.GetLIDs(f.MIDs, f.RIDs)

		inverse := inverseLIDs(unmapped, inverser, minLID, maxLID)
		nodes = append(nodes, node.NewStatic(inverse, order.IsReverse()))
	}
	return nodes
}

func inverseLIDs(unmapped []uint32, inv *inverser, minLID, maxLID uint32) []uint32 {
	result := make([]uint32, 0, len(unmapped))
	for _, v := range unmapped {
		// we skip those values that are not in the inverser, because such values appeared after the search query started
		if val, ok := inv.Inverse(v); ok {
			if minLID <= uint32(val) && uint32(val) <= maxLID {
				result = append(result, uint32(val))
			}
		}
	}
	return result
}

type ActiveIDsIndex struct {
	mids     []uint64
	rids     []uint64
	inverser *inverser
}

func (ii *ActiveIDsIndex) GetMID(lid seq.LID) seq.MID {
	restoredLID := ii.inverser.Revert(uint32(lid))
	return seq.MID(ii.mids[restoredLID])
}

func (ii *ActiveIDsIndex) GetRID(lid seq.LID) seq.RID {
	restoredLID := ii.inverser.Revert(uint32(lid))
	return seq.RID(ii.rids[restoredLID])
}

func (ii *ActiveIDsIndex) Len() int {
	return ii.inverser.Len()
}

func (ii *ActiveIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	checkedMID := ii.GetMID(lid)
	if checkedMID == id.MID {
		return ii.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

type ActiveDocsIndex struct {
	blocksOffsets []uint64
	docsPositions *DocsPositions
	docsReader    *disk.DocsReader
}

func (di *ActiveDocsIndex) GetBlocksOffsets(num uint32) uint64 {
	return di.blocksOffsets[num]
}

func (di *ActiveDocsIndex) GetDocPos(ids []seq.ID) []DocPos {
	docsPos := make([]DocPos, len(ids))
	for i, id := range ids {
		docsPos[i] = di.docsPositions.GetSync(id)
	}
	return docsPos
}

func (di *ActiveDocsIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return di.docsReader.ReadDocs(blockOffset, docOffsets)
}
