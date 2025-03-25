package active2

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/frac/fetcher"
	"github.com/ozontech/seq-db/frac/searcher"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
)

var (
	fetcherActiveStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "active_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

	activeAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	activeHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	activeRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_active_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
)

type DataProvider struct {
	f       Active2
	ctx     context.Context
	indexes []*Index
}

func (dp *DataProvider) Fetch(ids []seq.ID) ([][]byte, error) {
	sw := stopwatch.New()
	res := make([][]byte, len(ids))
	for _, index := range dp.indexes {
		fetchIndex := fetchIndex{
			index:      index,
			docsReader: dp.f.docsReader,
		}
		if err := fetcher.IndexFetch(ids, sw, &fetchIndex, res); err != nil {
			return nil, err
		}
	}
	sw.Export(fetcherActiveStagesSeconds)
	return res, nil
}

func (dp *DataProvider) Search(_ context.Context, params searcher.Params) (*seq.QPR, error) {
	s := searcher.New(dp.ctx, params, dp.f.cfg.searchCfg.AggLimits, getActiveSearchMetric(params))
	for _, index := range dp.indexes {
		if !index.IsIntersecting(params.From, params.To) {
			continue
		}
		if err := s.IndexSearch(&searchIndex{ctx: dp.ctx, index: index}); err != nil {
			return nil, err
		}
	}
	qpr := s.GetResult()
	qpr.IDs.ApplyHint(dp.f.Info().Name())
	return qpr, nil
}

func getActiveSearchMetric(params searcher.Params) *prometheus.HistogramVec {
	if params.HasAgg() {
		return activeAggSearchSec
	}
	if params.HasHist() {
		return activeHistSearchSec
	}
	return activeRegSearchSec
}
