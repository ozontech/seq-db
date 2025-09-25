package frac

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

type DataProvider interface {
	Fetch([]seq.ID) ([][]byte, error)
	Search(processor.SearchParams) (*seq.QPR, error)
}

type Fraction interface {
	Info() *Info
	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
	DataProvider(context.Context) (DataProvider, func())
	Offload(ctx context.Context, u storage.Uploader) (bool, error)
	Suicide()
}

var (
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "fraction_stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage", "fraction_type"})
	fractionAggSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_fraction_agg_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage", "fraction_type"})
	fractionHistSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_fraction_hist_search_sec",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage", "fraction_type"})
	fractionRegSearchSec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "search",
		Name:      "tracer_fraction_reg_search_sec",
		Buckets:   metric.SecondsBuckets,
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
	return fractionRegSearchSec
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
