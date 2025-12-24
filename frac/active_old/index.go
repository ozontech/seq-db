package active_old

import (
	"context"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

type dataProvider struct {
	ctx    context.Context
	config *frac.Config
	info   *frac.Info

	mids *UInt64s
	rids *UInt64s

	tokenList *tokenList

	blocksOffsets []uint64
	docsPositions *DocsPositions
	docsReader    *storage.DocsReader

	idsIndex *idsIndex
}

func (dp *dataProvider) release() {
	if dp.idsIndex != nil {
		dp.idsIndex.inverser.Release()
	}
}

// getIDsIndex creates on demand and returns ActiveIDsIndex.
// Creation of inverser for ActiveIDsIndex is expensive operation
func (dp *dataProvider) getIDsIndex() *idsIndex {
	if dp.idsIndex == nil {
		// creation order is matter
		mapping := dp.tokenList.GetAllTokenLIDs().GetLIDs(dp.mids, dp.rids)
		mids := dp.mids.GetVals() // mids and rids should be created after mapping to ensure that
		rids := dp.rids.GetVals() // they contain all the ids that mapping contains.
		dp.idsIndex = &idsIndex{
			inverser: newInverser(mapping, len(mids)),
			mids:     mids,
			rids:     rids,
		}
	}
	return dp.idsIndex
}

func (dp *dataProvider) getTokenIndex() *tokenIndex {
	return &tokenIndex{
		ctx:       dp.ctx,
		mids:      dp.mids,
		rids:      dp.rids,
		tokenList: dp.tokenList,
		inverser:  dp.getIDsIndex().inverser,
	}
}

func (dp *dataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()

	defer sw.Export(
		frac.FetcherStagesSeconds,
		stopwatch.SetLabel("fraction_type", "active"),
	)

	res := make([][]byte, len(ids))

	indexes := []fetchIndex{{
		blocksOffsets: dp.blocksOffsets,
		docsPositions: dp.docsPositions,
		docsReader:    dp.docsReader,
	}}

	for _, fi := range indexes {
		if err := processor.IndexFetch(ids, sw, &fi, res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (dp *dataProvider) Search(params processor.SearchParams) (*seq.QPR, error) {
	// The index of the active fraction changes in parts and at a single moment in time may not be consistent.
	// So we can add new IDs to the index but update the range [from; to] with a delay.
	// Because of this, at the Search stage, we can get IDs that are outside the fraction range [from; to].
	//
	// Because of this, at the next Fetch stage, we may not find documents with such IDs, because we will ignore
	// the fraction whose range [from; to] does not contain this ID.
	//
	// To prevent this from happening, so that the Search stage and the Fetch stage work consistently,
	// we must limit the query range in accordance with the current fraction range [from; to].
	params.From = max(params.From, dp.info.From)
	params.To = min(params.To, dp.info.To)

	aggLimits := processor.AggLimits(dp.config.Search.AggLimits)

	sw := stopwatch.New()

	defer sw.Export(
		frac.FractionSearchMetric(params),
		stopwatch.SetLabel("fraction_type", "active"),
	)

	t := sw.Start("total")

	m := sw.Start("new_search_index")
	indexes := []searchIndex{{
		idsIndex:   dp.getIDsIndex(),
		tokenIndex: dp.getTokenIndex(),
	}}
	m.Stop()

	qprs := make([]*seq.QPR, 0, len(indexes))

	for _, si := range indexes {
		qpr, err := processor.IndexSearch(dp.ctx, params, &si, aggLimits, sw)
		if err != nil {
			return nil, err
		}
		qprs = append(qprs, qpr)
	}

	res := processor.MergeQPRs(qprs, params)
	res.IDs.ApplyHint(dp.info.Name())
	t.Stop()

	return res, nil
}

type idsIndex struct {
	mids     []uint64
	rids     []uint64
	inverser *inverser
}

func (p *idsIndex) GetMID(lid seq.LID) seq.MID {
	restoredLID := p.inverser.Revert(uint32(lid))
	return seq.MID(p.mids[restoredLID])
}

func (p *idsIndex) GetRID(lid seq.LID) seq.RID {
	restoredLID := p.inverser.Revert(uint32(lid))
	return seq.RID(p.rids[restoredLID])
}

func (p *idsIndex) Len() int {
	return p.inverser.Len()
}

func (p *idsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	checkedMID := p.GetMID(lid)
	if checkedMID == id.MID {
		return p.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}

type searchIndex struct {
	*idsIndex
	*tokenIndex
}

type tokenIndex struct {
	ctx       context.Context
	mids      *UInt64s
	rids      *UInt64s
	tokenList *tokenList
	inverser  *inverser
}

func (si *tokenIndex) GetValByTID(tid uint32) []byte {
	return si.tokenList.GetValByTID(tid)
}

func (si *tokenIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	return si.tokenList.FindPattern(si.ctx, t)
}

func (si *tokenIndex) GetLIDsFromTIDs(tids []uint32, _ lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	nodes := make([]node.Node, 0, len(tids))
	for _, tid := range tids {
		tlids := si.tokenList.Provide(tid)
		unmapped := tlids.GetLIDs(si.mids, si.rids)
		inverse := inverseLIDs(unmapped, si.inverser, minLID, maxLID)
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

type fetchIndex struct {
	blocksOffsets []uint64
	docsPositions *DocsPositions
	docsReader    *storage.DocsReader
}

func (di *fetchIndex) GetBlocksOffsets(num uint32) uint64 {
	return di.blocksOffsets[num]
}

func (di *fetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	docsPos := make([]seq.DocPos, len(ids))
	for i, id := range ids {
		docsPos[i] = di.docsPositions.GetSync(id)
	}
	return docsPos
}

func (di *fetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return di.docsReader.ReadDocs(blockOffset, docOffsets)
}
