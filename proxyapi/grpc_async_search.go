package proxyapi

import (
	"context"
	"fmt"
	"time"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (g *grpcV1) StartAsyncSearch(ctx context.Context, r *seqproxyapi.StartAsyncSearchRequest) (*seqproxyapi.StartAsyncSearchResponse, error) {
	aggs, err := convertAggsQuery(r.Aggs)
	if err != nil {
		return nil, err
	}

	var histInterval time.Duration
	if r.Hist != nil {
		histInterval, err = util.ParseDuration(r.Hist.Interval)
		if err != nil {
			return nil, fmt.Errorf("error parsing hist interval: %w", err)
		}
	}

	resp, err := g.searchIngestor.StartAsyncSearch(ctx, search.AsyncRequest{
		Retention:         r.Retention.AsDuration(),
		Query:             r.GetQuery().GetQuery(),
		From:              r.GetQuery().GetFrom().AsTime(),
		To:                r.GetQuery().GetTo().AsTime(),
		Aggregations:      aggs,
		HistogramInterval: seq.MID(histInterval.Milliseconds()),
		WithDocs:          r.WithDocs,
	})
	if err != nil {
		return nil, err
	}
	return &seqproxyapi.StartAsyncSearchResponse{
		SearchId: resp.ID,
	}, nil
}

func (g *grpcV1) FetchAsyncSearchResult(ctx context.Context, r *seqproxyapi.FetchAsyncSearchResultRequest) (*seqproxyapi.FetchAsyncSearchResultResponse, error) {
	resp, stream, err := g.searchIngestor.FetchAsyncSearchResult(ctx, search.FetchAsyncSearchResultRequest{
		ID:     r.SearchId,
		Size:   int(r.Size),
		Offset: int(r.Offset),
	})
	if err != nil {
		return nil, err
	}

	canceledAt := timestamppb.New(resp.CanceledAt)
	if resp.CanceledAt.IsZero() {
		canceledAt = nil
	}

	docs := makeProtoDocs(&resp.QPR, stream)

	return &seqproxyapi.FetchAsyncSearchResultResponse{
		Status: seqproxyapi.MustProtoAsyncSearchStatus(resp.Status),
		Response: &seqproxyapi.ComplexSearchResponse{
			Total:   int64(resp.QPR.Total),
			Docs:    docs,
			Aggs:    makeProtoAggregation(resp.AggResult),
			Hist:    makeProtoHistogram(&resp.QPR),
			Error:   nil,
			Explain: nil,
		},
		StartedAt:  timestamppb.New(resp.StartedAt),
		ExpiresAt:  timestamppb.New(resp.ExpiresAt),
		CanceledAt: canceledAt,
		Progress:   resp.Progress,
		DiskUsage:  resp.DiskUsage,
	}, nil
}

func (g *grpcV1) CancelAsyncSearch(ctx context.Context, r *seqproxyapi.CancelAsyncSearchRequest) (*seqproxyapi.CancelAsyncSearchResponse, error) {
	if err := g.searchIngestor.CancelAsyncSearch(ctx, r.SearchId); err != nil {
		return nil, fmt.Errorf("cancelling search: %s", err)
	}
	return &seqproxyapi.CancelAsyncSearchResponse{}, nil
}

func (g *grpcV1) DeleteAsyncSearch(ctx context.Context, r *seqproxyapi.DeleteAsyncSearchRequest) (*seqproxyapi.DeleteAsyncSearchResponse, error) {
	if err := g.searchIngestor.DeleteAsyncSearch(ctx, r.SearchId); err != nil {
		return nil, fmt.Errorf("deleting search: %s", err)
	}
	return &seqproxyapi.DeleteAsyncSearchResponse{}, nil
}
