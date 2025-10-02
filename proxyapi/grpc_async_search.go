package proxyapi

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

func (g *grpcV1) StartAsyncSearch(
	ctx context.Context,
	r *seqproxyapi.StartAsyncSearchRequest,
) (*seqproxyapi.StartAsyncSearchResponse, error) {
	if g.config.AsyncSearchMaxDocumentsPerRequest > 0 && r.Size > g.config.AsyncSearchMaxDocumentsPerRequest {
		return nil, status.Errorf(codes.InvalidArgument, "too many documents are requested: count=%d, max=%d",
			r.Size, g.config.AsyncSearchMaxDocumentsPerRequest)
	}

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
		Size:              r.Size,
	})
	if err != nil {
		return nil, err
	}
	return &seqproxyapi.StartAsyncSearchResponse{
		SearchId: resp.ID,
	}, nil
}

func (g *grpcV1) FetchAsyncSearchResult(
	ctx context.Context,
	r *seqproxyapi.FetchAsyncSearchResultRequest,
) (*seqproxyapi.FetchAsyncSearchResultResponse, error) {
	resp, stream, err := g.searchIngestor.FetchAsyncSearchResult(ctx, search.FetchAsyncSearchResultRequest{
		ID:     r.SearchId,
		Size:   int(r.Size),
		Offset: int(r.Offset),
		Order:  r.Order.MustDocsOrder(),
	})
	if err != nil {
		return nil, err
	}

	var canceledAt *timestamppb.Timestamp
	if !resp.CanceledAt.IsZero() {
		canceledAt = timestamppb.New(resp.CanceledAt)
	}

	docs := makeProtoDocs(&resp.QPR, stream)

	searchReq := &seqproxyapi.StartAsyncSearchRequest{
		Retention: durationpb.New(resp.Request.Retention),
		Query: &seqproxyapi.SearchQuery{
			Query: resp.Request.Query,
			From:  timestamppb.New(resp.Request.From),
			To:    timestamppb.New(resp.Request.To),
		},
		Aggs:     makeProtoRequestAggregations(resp.Request.Aggregations),
		WithDocs: resp.Request.WithDocs,
		Size:     resp.Request.Size,
	}
	if resp.Request.HistogramInterval > 0 {
		searchReq.Hist = &seqproxyapi.HistQuery{
			Interval: seq.MIDToDuration(resp.Request.HistogramInterval).String(),
		}
	}

	return &seqproxyapi.FetchAsyncSearchResultResponse{
		Status:  seqproxyapi.MustProtoAsyncSearchStatus(resp.Status),
		Request: searchReq,
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

func (g *grpcV1) GetAsyncSearchesList(
	ctx context.Context,
	r *seqproxyapi.GetAsyncSearchesListRequest,
) (*seqproxyapi.GetAsyncSearchesListResponse, error) {
	var searchStatus *fracmanager.AsyncSearchStatus
	if r.Status != nil {
		s := r.Status.MustAsyncSearchStatus()
		searchStatus = &s
	}

	req := search.GetAsyncSearchesListRequest{
		Status: searchStatus,
		Size:   int(r.Size),
		Offset: int(r.Offset),
		IDs:    r.Ids,
	}

	searches, err := g.searchIngestor.GetAsyncSearchesList(ctx, req)
	if err != nil {
		return nil, err
	}

	return &seqproxyapi.GetAsyncSearchesListResponse{
		Searches: makeProtoAsyncSearchesList(searches),
	}, nil
}

func (g *grpcV1) CancelAsyncSearch(
	ctx context.Context,
	r *seqproxyapi.CancelAsyncSearchRequest,
) (*seqproxyapi.CancelAsyncSearchResponse, error) {
	if err := g.searchIngestor.CancelAsyncSearch(ctx, r.SearchId); err != nil {
		return nil, fmt.Errorf("cancelling search: %s", err)
	}
	return &seqproxyapi.CancelAsyncSearchResponse{}, nil
}

func (g *grpcV1) DeleteAsyncSearch(
	ctx context.Context,
	r *seqproxyapi.DeleteAsyncSearchRequest,
) (*seqproxyapi.DeleteAsyncSearchResponse, error) {
	if err := g.searchIngestor.DeleteAsyncSearch(ctx, r.SearchId); err != nil {
		return nil, fmt.Errorf("deleting search: %s", err)
	}
	return &seqproxyapi.DeleteAsyncSearchResponse{}, nil
}

func makeProtoRequestAggregations(sourceAggs []search.AggQuery) []*seqproxyapi.AggQuery {
	aggs := make([]*seqproxyapi.AggQuery, 0, len(sourceAggs))
	for _, agg := range sourceAggs {
		aggs = append(aggs, &seqproxyapi.AggQuery{
			Field:     agg.Field,
			GroupBy:   agg.GroupBy,
			Func:      seqproxyapi.AggFunc(agg.Func),
			Quantiles: agg.Quantiles,
		})
	}
	return aggs
}

func makeProtoAsyncSearchesList(in []*search.AsyncSearchesListItem) []*seqproxyapi.AsyncSearchesListItem {
	searches := make([]*seqproxyapi.AsyncSearchesListItem, 0, len(in))
	for _, s := range in {
		var canceledAt *timestamppb.Timestamp
		if !s.CanceledAt.IsZero() {
			canceledAt = timestamppb.New(s.CanceledAt)
		}

		searchReq := &seqproxyapi.StartAsyncSearchRequest{
			Retention: durationpb.New(s.Request.Retention),
			Query: &seqproxyapi.SearchQuery{
				Query: s.Request.Query,
				From:  timestamppb.New(s.Request.From),
				To:    timestamppb.New(s.Request.To),
			},
			Aggs:     makeProtoRequestAggregations(s.Request.Aggregations),
			WithDocs: s.Request.WithDocs,
			Size:     s.Request.Size,
		}
		if s.Request.HistogramInterval > 0 {
			searchReq.Hist = &seqproxyapi.HistQuery{
				Interval: seq.MIDToDuration(s.Request.HistogramInterval).String(),
			}
		}

		searches = append(searches, &seqproxyapi.AsyncSearchesListItem{
			SearchId:   s.ID,
			Status:     seqproxyapi.MustProtoAsyncSearchStatus(s.Status),
			Request:    searchReq,
			StartedAt:  timestamppb.New(s.StartedAt),
			ExpiresAt:  timestamppb.New(s.ExpiresAt),
			CanceledAt: canceledAt,
			Progress:   s.Progress,
			DiskUsage:  s.DiskUsage,
		})
	}

	return searches
}
