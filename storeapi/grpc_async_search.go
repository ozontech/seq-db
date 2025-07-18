package storeapi

import (
	"context"
	"math"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (g *GrpcV1) StartAsyncSearch(_ context.Context, r *storeapi.StartAsyncSearchRequest) (*storeapi.StartAsyncSearchResponse, error) {
	aggs, err := aggQueriesFromProto(r.Aggs)
	if err != nil {
		return nil, err
	}

	limit := 0
	if r.WithDocs {
		limit = math.MaxInt
	}

	params := processor.SearchParams{
		AST:          nil, // Parse AST later.
		AggQ:         aggs,
		HistInterval: uint64(r.HistogramInterval),
		From:         seq.MID(r.From),
		To:           seq.MID(r.To),
		Limit:        limit,
		WithTotal:    false,
		Order:        seq.DocsOrderDesc,
	}

	req := fracmanager.AsyncSearchRequest{
		ID:        r.SearchId,
		Query:     r.Query,
		Params:    params,
		Retention: r.Retention.AsDuration(),
	}
	fracs := g.fracManager.GetAllFracs().FilterInRange(seq.MID(r.From), seq.MID(r.To))
	if err := g.asyncSearcher.StartSearch(req, fracs); err != nil {
		return nil, err
	}

	return &storeapi.StartAsyncSearchResponse{}, nil
}

func (g *GrpcV1) FetchAsyncSearchResult(_ context.Context, r *storeapi.FetchAsyncSearchResultRequest) (*storeapi.FetchAsyncSearchResultResponse, error) {
	fr, exists := g.asyncSearcher.FetchSearchResult(fracmanager.FetchSearchResultRequest{
		ID:    r.SearchId,
		Limit: int(r.Size + r.Offset),
		Order: r.Order.MustDocsOrder(),
	})
	if !exists {
		return nil, status.Error(codes.NotFound, "search not found")
	}

	resp := buildSearchResponse(&fr.QPR)

	var canceledAt *timestamppb.Timestamp
	if fr.CanceledAt.IsZero() {
		canceledAt = timestamppb.New(fr.CanceledAt)
	}

	return &storeapi.FetchAsyncSearchResultResponse{
		Status:            storeapi.MustProtoAsyncSearchStatus(fr.Status),
		Response:          resp,
		StartedAt:         timestamppb.New(fr.StartedAt),
		ExpiresAt:         timestamppb.New(fr.ExpiresAt),
		CanceledAt:        canceledAt,
		FracsDone:         uint64(fr.FracsDone),
		FracsQueue:        uint64(fr.FracsInQueue),
		DiskUsage:         uint64(fr.DiskUsage),
		Aggs:              convertAggQueriesToProto(fr.AggQueries),
		HistogramInterval: int64(fr.HistInterval),
	}, nil
}

func (g *GrpcV1) CancelAsyncSearch(_ context.Context, r *storeapi.CancelAsyncSearchRequest) (*storeapi.CancelAsyncSearchResponse, error) {
	g.asyncSearcher.CancelSearch(r.SearchId)
	return &storeapi.CancelAsyncSearchResponse{}, nil
}

func (g *GrpcV1) DeleteAsyncSearch(_ context.Context, r *storeapi.DeleteAsyncSearchRequest) (*storeapi.DeleteAsyncSearchResponse, error) {
	g.asyncSearcher.DeleteSearch(r.SearchId)
	return &storeapi.DeleteAsyncSearchResponse{}, nil
}

func convertAggQueriesToProto(query []processor.AggQuery) []*storeapi.AggQuery {
	var res []*storeapi.AggQuery
	for _, q := range query {
		pq := &storeapi.AggQuery{
			Func:      storeapi.MustProtoAggFunc(q.Func),
			Quantiles: q.Quantiles,
		}
		if q.Field != nil {
			pq.Field = q.Field.Field
		}
		if q.GroupBy != nil {
			pq.GroupBy = q.GroupBy.Field
		}
		res = append(res, pq)
	}
	return res
}
