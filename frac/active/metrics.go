package active

import (
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	fetcherStagesSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "active_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})

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

	bulkStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "bulk",
		Name:      "stages_seconds2",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
)

// getActiveSearchMetric selects a Prometheus metric depending on the type of search query.
func getActiveSearchMetric(params processor.SearchParams) *prometheus.HistogramVec {
	if params.HasAgg() {
		return searchAggSec
	}
	if params.HasHist() {
		return searchHstSec
	}
	return searchSimpleSec
}
