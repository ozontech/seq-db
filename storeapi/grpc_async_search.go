package storeapi

import (
	"context"
	"math"
	"time"

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

	params := processor.SearchParams{
		AST:          nil, // Parse AST later.
		AggQ:         aggs,
		HistInterval: uint64(r.HistogramInterval),
		From:         seq.MID(r.From),
		To:           seq.MID(r.To),
		Limit:        math.MaxInt32, // TODO: use WithDocs from request
		WithTotal:    false,
		Order:        r.Order.MustDocsOrder(),
	}

	req := fracmanager.AsyncSearchRequest{
		ID:        r.SearchId,
		Query:     r.Query,
		Params:    params,
		Retention: time.Hour * 24, // todo: use value from request
	}
	if err := g.asyncSearcher.StartSearch(req); err != nil {
		return nil, err
	}

	return &storeapi.StartAsyncSearchResponse{}, nil
}

func (g *GrpcV1) FetchAsyncSearchResult(_ context.Context, r *storeapi.FetchAsyncSearchResultRequest) (*storeapi.FetchAsyncSearchResultResponse, error) {
	fetchResp, exists := g.asyncSearcher.FetchSearchResult(fracmanager.FetchSearchResultRequest{ID: r.SearchId})
	if !exists {
		return nil, status.Error(codes.NotFound, "search not found")
	}

	resp := buildSearchResponse(&fetchResp.QPR)

	return &storeapi.FetchAsyncSearchResultResponse{
		Done:              fetchResp.Done,
		Response:          resp,
		Expiration:        timestamppb.New(fetchResp.Expiration),
		Aggs:              convertAggQueriesToProto(fetchResp.AggQueries),
		HistogramInterval: int64(fetchResp.HistInterval),
		Order:             storeapi.MustProtoOrder(fetchResp.Order),
	}, nil
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
