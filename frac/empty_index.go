package frac

import (
	"context"

	"github.com/ozontech/seq-db/frac/searcher"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
)

type EmptyIndexProvider struct{}

func (EmptyIndexProvider) Fetch([]seq.ID) ([][]byte, error) { return nil, nil }

func (EmptyIndexProvider) Search(context.Context, searcher.Params) (*seq.QPR, error) {
	metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
	return nil, nil
}
