package frac

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
)

type Fraction interface {
	Info() *common.Info
	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
	Fetch(context.Context, []seq.ID) ([][]byte, error)
	Search(context.Context, processor.SearchParams) (*seq.QPR, error)
	FindLIDs(context.Context, []seq.ID) ([]seq.LID, error) // TODO: ???
}

var (
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "fraction_stages_seconds",
		Buckets:   metric.SecondsBuckets,
		Help:      "Fetch processing time by stage",
	}, []string{"stage", "fraction_type"})
	fractionAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "fraction_agg_search_seconds",
		Buckets:   metric.SecondsBuckets,
		Help:      "Search with aggregation processing time by stage",
	}, []string{"stage", "fraction_type"})
	fractionHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "fraction_hist_search_seconds",
		Buckets:   metric.SecondsBuckets,
		Help:      "Search with histogram processing time by stage",
	}, []string{"stage", "fraction_type"})
	fractionRegularSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "fraction_regular_search_seconds",
		Buckets:   metric.SecondsBuckets,
		Help:      "Regular search processing time by stage",
	}, []string{"stage", "fraction_type"})
)

func fractionSearchMetric(
	params processor.SearchParams,
) *prometheus.HistogramVec {
	if params.HasAgg() {
		return fractionAggSearchSec
	}
	if params.HasHist() {
		return fractionHistSearchSec
	}
	return fractionRegularSearchSec
}

func fracToString(f Fraction, fracType string) string {
	info := f.Info()
	s := fmt.Sprintf(
		"%s fraction name=%s, creation time=%s, from=%s, to=%s, %s",
		fracType,
		info.Name(),
		time.UnixMilli(int64(info.CreationTime)).Format(consts.ESTimeFormat),
		info.From,
		info.To,
		info.String(),
	)
	if fracType == "" {
		return s[1:]
	}
	return s
}
