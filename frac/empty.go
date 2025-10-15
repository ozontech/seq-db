package frac

import (
	"context"
	"math"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

var EmptyFraction Fraction = Empty{
	info: &common.Info{
		Path: "empty",
		From: math.MaxUint64,
		To:   0,
	},
}

type Empty struct {
	info *common.Info
}

func (Empty) Fetch(context.Context, []seq.ID) ([][]byte, error) {
	metric.CountersTotal.WithLabelValues("empty_fraction_fetch").Inc()
	return nil, nil
}

func (Empty) Search(_ context.Context, params processor.SearchParams) (*seq.QPR, error) {
	metric.CountersTotal.WithLabelValues("empty_fraction_search").Inc()
	return &seq.QPR{Aggs: make([]seq.AggregatableSamples, len(params.AggQ))}, nil
}

func (e Empty) Info() *common.Info {
	return e.info
}
func (Empty) IsIntersecting(seq.MID, seq.MID) bool {
	return false
}
func (Empty) Contains(mid seq.MID) bool {
	return false
}

func (Empty) Offload(ctx context.Context, u storage.Uploader) (bool, error) {
	return false, nil
}

func (Empty) Suicide() {}
