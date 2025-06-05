package active2

import (
	"context"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
)

type dataProvider struct {
	ctx context.Context

	config *frac.Config

	info       *frac.Info
	indexes    []*memIndex
	docsReader *disk.DocsReader
}

var fetcherStagesSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "seq_db_store",
	Subsystem: "fetcher",
	Name:      "active_stages_seconds",
	Buckets:   metric.SecondsBuckets,
}, []string{"stage"})

func (dp *dataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	defer sw.Export(fetcherStagesSec)

	t := sw.Start("total")
	res := make([][]byte, len(ids))
	for _, index := range dp.indexes {
		fetchIndex := fetchIndex{index: index, docsReader: dp.docsReader}
		if err := processor.IndexFetch(ids, sw, &fetchIndex, res); err != nil {
			return nil, err
		}
	}
	t.Stop()

	return res, nil
}

func (dp *dataProvider) Search(params processor.SearchParams) (*seq.QPR, error) {
	sw := stopwatch.New()
	defer sw.Export(getActiveSearchMetric(params))

	t := sw.Start("total")
	qprs := make([]*seq.QPR, 0, len(dp.indexes))
	aggLimits := processor.AggLimits(dp.config.Search.AggLimits)
	for _, index := range dp.indexes {
		si := searchIndex{ctx: dp.ctx, index: index}
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

type fetchIndex struct {
	index      *memIndex
	docsReader *disk.DocsReader
}

func (si *fetchIndex) GetBlocksOffsets(blockIndex uint32) uint64 {
	return si.index.blocksOffsets[blockIndex]
}

func (si *fetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	docsPos := make([]seq.DocPos, len(ids))
	for i, id := range ids {
		var ok bool
		if docsPos[i], ok = si.index.positions[id]; !ok {
			docsPos[i] = seq.DocPosNotFound
		}
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

var (
	searchAggSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	searchHstSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	searchSimpleSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
)

func getActiveSearchMetric(params processor.SearchParams) *prometheus.HistogramVec {
	if params.HasAgg() {
		return searchAggSec
	}
	if params.HasHist() {
		return searchHstSec
	}
	return searchSimpleSec
}
