package storeapi

import (
	"context"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
)

func (g *GrpcV1) StartAsyncSearch(
	_ context.Context,
	r *storeapi.StartAsyncSearchRequest,
) (*storeapi.StartAsyncSearchResponse, error) {
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
		WithTotal:    r.WithDocs, // return total if docs needed
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

func (g *GrpcV1) FetchAsyncSearchResult(
	_ context.Context,
	r *storeapi.FetchAsyncSearchResultRequest,
) (*storeapi.FetchAsyncSearchResultResponse, error) {
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
	if !fr.CanceledAt.IsZero() {
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
		Query:             fr.Query,
		From:              timestamppb.New(fr.From.Time()),
		To:                timestamppb.New(fr.To.Time()),
		Retention:         durationpb.New(fr.Retention),
		WithDocs:          fr.WithDocs,
	}, nil
}

func (g *GrpcV1) CancelAsyncSearch(
	_ context.Context,
	r *storeapi.CancelAsyncSearchRequest,
) (*storeapi.CancelAsyncSearchResponse, error) {
	g.asyncSearcher.CancelSearch(r.SearchId)
	return &storeapi.CancelAsyncSearchResponse{}, nil
}

func (g *GrpcV1) DeleteAsyncSearch(
	_ context.Context,
	r *storeapi.DeleteAsyncSearchRequest,
) (*storeapi.DeleteAsyncSearchResponse, error) {
	g.asyncSearcher.DeleteSearch(r.SearchId)
	return &storeapi.DeleteAsyncSearchResponse{}, nil
}

func (g *GrpcV1) GetAsyncSearchesList(
	_ context.Context,
	r *storeapi.GetAsyncSearchesListRequest,
) (*storeapi.GetAsyncSearchesListResponse, error) {
	var searchStatus *fracmanager.AsyncSearchStatus
	if r.Status != nil {
		s := r.Status.MustAsyncSearchStatus()
		searchStatus = &s
	}

	searches := g.asyncSearcher.GetAsyncSearchesList(fracmanager.GetAsyncSearchesListRequest{
		Status: searchStatus,
		IDs:    r.Ids,
	})

	return &storeapi.GetAsyncSearchesListResponse{
		Searches: convertAsyncSearchesToProto(searches),
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

func convertAsyncSearchesToProto(in []*fracmanager.AsyncSearchesListItem) []*storeapi.AsyncSearchesListItem {
	res := make([]*storeapi.AsyncSearchesListItem, 0, len(in))

	for _, s := range in {
		var canceledAt *timestamppb.Timestamp
		if !s.CanceledAt.IsZero() {
			canceledAt = timestamppb.New(s.CanceledAt)
		}

		res = append(res, &storeapi.AsyncSearchesListItem{
			SearchId:          s.ID,
			Status:            storeapi.MustProtoAsyncSearchStatus(s.Status),
			StartedAt:         timestamppb.New(s.StartedAt),
			ExpiresAt:         timestamppb.New(s.ExpiresAt),
			CanceledAt:        canceledAt,
			FracsDone:         uint64(s.FracsDone),
			FracsQueue:        uint64(s.FracsInQueue),
			DiskUsage:         uint64(s.DiskUsage),
			Aggs:              convertAggQueriesToProto(s.AggQueries),
			HistogramInterval: int64(s.HistInterval),
			Query:             s.Query,
			From:              timestamppb.New(s.From.Time()),
			To:                timestamppb.New(s.To.Time()),
			Retention:         durationpb.New(s.Retention),
			WithDocs:          s.WithDocs,
		})
	}

	return res
}
