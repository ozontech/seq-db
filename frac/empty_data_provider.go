package frac

import (
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
)

type EmptyDataProvider struct{}

func (EmptyDataProvider) Fetch([]seq.ID) ([][]byte, error) { return nil, nil }

func (EmptyDataProvider) Search(params processor.SearchParams) (*seq.QPR, error) {
	metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
	return &seq.QPR{Aggs: make([]seq.QPRHistogram, len(params.AggQ))}, nil
}
