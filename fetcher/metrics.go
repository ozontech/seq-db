package fetcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

var (
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	fetcherIDsPerFraction = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "ids_per_fraction",
	})
	fetcherWithHints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_with_hints",
	})
	fetcherWithoutHint = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_without_hints",
	})
	fetcherHintMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "hint_misses",
	})

	fetcherActiveStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "active_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	fetcherSealedStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "sealed_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
)

func getStagesMetric(fracType string) *prometheus.HistogramVec {
	if fracType == frac.TypeActive {
		return fetcherActiveStagesSeconds
	}
	return fetcherSealedStagesSeconds
}
