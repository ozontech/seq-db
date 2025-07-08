package search

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/seq"
)

type AsyncRequest struct {
	Retention         time.Duration
	Query             string
	From              time.Time
	To                time.Time
	Aggregations      []AggQuery
	HistogramInterval seq.MID
	WithDocs          bool
}

type AsyncResponse struct {
	ID string
}

func (si *Ingestor) StartAsyncSearch(ctx context.Context, r AsyncRequest) (AsyncResponse, error) {
	requestID := uuid.New().String()

	searchStores, err := si.getAsyncSearchStores()
	if err != nil {
		return AsyncResponse{}, err
	}

	req := storeapi.StartAsyncSearchRequest{
		SearchId:          requestID,
		Query:             r.Query,
		From:              r.From.UnixMilli(),
		To:                r.To.UnixMilli(),
		Aggs:              convertToAggsQuery(r.Aggregations),
		HistogramInterval: int64(r.HistogramInterval),
		Retention:         durationpb.New(r.Retention),
		WithDocs:          r.WithDocs,
	}
	for i, shard := range searchStores.Shards {
		var err error

		// todo shuffle
		for _, replica := range shard {
			_, err = si.clients[replica].StartAsyncSearch(ctx, &req)
			if err != nil {
				logger.Error("Can't start async search",
					zap.String("replica", replica), zap.Error(err))
				continue
			}
			break
		}
		if err != nil {
			return AsyncResponse{}, fmt.Errorf("starting search in shard=%d: %s", i, err)
		}
	}

	return AsyncResponse{ID: requestID}, nil
}

type FetchAsyncSearchResultRequest struct {
	ID     string
	Size   int
	Offset int
	Order  seq.DocsOrder
}

type FetchAsyncSearchResultResponse struct {
	Status     fracmanager.AsyncSearchStatus
	QPR        seq.QPR
	CanceledAt time.Time

	StartedAt time.Time
	ExpiresAt time.Time

	Progress  float64
	DiskUsage uint64

	AggResult []seq.AggregationResult

	Request AsyncRequest
}

type GetAsyncSearchesListRequest struct {
	Status *fracmanager.AsyncSearchStatus
	Size   int
	Offset int
}

type AsyncSearchesListItem struct {
	ID     string
	Status fracmanager.AsyncSearchStatus

	StartedAt  time.Time
	ExpiresAt  time.Time
	CanceledAt time.Time

	Progress  float64
	DiskUsage uint64

	Request AsyncRequest
}

func (si *Ingestor) FetchAsyncSearchResult(
	ctx context.Context,
	r FetchAsyncSearchResultRequest,
) (FetchAsyncSearchResultResponse, DocsIterator, error) {
	searchStores, err := si.getAsyncSearchStores()
	if err != nil {
		return FetchAsyncSearchResultResponse{}, nil, err
	}

	req := storeapi.FetchAsyncSearchResultRequest{
		SearchId: r.ID,
		Size:     int32(r.Size),
		Offset:   int32(r.Offset),
	}

	fracsDone := 0
	fracsInQueue := 0
	histInterval := seq.MID(0)
	pr := FetchAsyncSearchResultResponse{}
	mergeStoreResp := func(sr *storeapi.FetchAsyncSearchResultResponse, replica string) {
		pr.DiskUsage += sr.DiskUsage
		fracsInQueue += int(sr.FracsQueue)
		fracsDone += int(sr.FracsDone)

		histInterval = seq.MID(sr.HistogramInterval)

		ss := sr.Status.MustAsyncSearchStatus()
		pr.Status = mergeAsyncSearchStatus(pr.Status, ss)

		for _, errStr := range sr.GetResponse().GetErrors() {
			pr.QPR.Errors = append(pr.QPR.Errors, seq.ErrorSource{
				ErrStr: errStr,
				Source: si.sourceByClient[replica],
			})
		}

		t := sr.ExpiresAt.AsTime()
		if pr.ExpiresAt.IsZero() || pr.ExpiresAt.After(t) {
			pr.ExpiresAt = t
		}
		t = sr.StartedAt.AsTime()
		if pr.StartedAt.IsZero() || pr.StartedAt.After(t) {
			pr.StartedAt = t
		}
		t = sr.CanceledAt.AsTime()
		if sr.CanceledAt != nil && (pr.CanceledAt.IsZero() || pr.CanceledAt.After(t)) {
			pr.CanceledAt = t
		}

		qpr := responseToQPR(sr.Response, si.sourceByClient[replica], false)
		seq.MergeQPRs(&pr.QPR, []*seq.QPR{qpr}, r.Size+r.Offset, histInterval, r.Order)
	}

	var aggQueries []seq.AggregateArgs
	var searchReq *AsyncRequest
	anyResponse := false
	for _, shard := range searchStores.Shards {
		for _, replica := range shard {
			storeResp, err := si.clients[replica].FetchAsyncSearchResult(ctx, &req)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					continue
				}
				return FetchAsyncSearchResultResponse{}, nil, err
			}
			anyResponse = true
			mergeStoreResp(storeResp, replica)

			if len(aggQueries) == 0 {
				for _, agg := range storeResp.Aggs {
					aggQueries = append(aggQueries, seq.AggregateArgs{
						Func:      agg.Func.MustAggFunc(),
						Quantiles: agg.Quantiles,
					})
				}
			}

			if searchReq == nil {
				reqAggs := make([]AggQuery, 0, len(storeResp.Aggs))
				for _, agg := range storeResp.Aggs {
					reqAggs = append(reqAggs, AggQuery{
						Field:     agg.Field,
						GroupBy:   agg.GroupBy,
						Func:      agg.Func.MustAggFunc(),
						Quantiles: agg.Quantiles,
					})
				}

				searchReq = &AsyncRequest{
					Retention:         storeResp.Retention.AsDuration(),
					Query:             storeResp.Query,
					From:              storeResp.From.AsTime(),
					To:                storeResp.To.AsTime(),
					Aggregations:      reqAggs,
					HistogramInterval: histInterval,
					WithDocs:          storeResp.WithDocs,
				}
			}

			break
		}
	}
	if !anyResponse {
		return FetchAsyncSearchResultResponse{}, nil, status.Error(codes.NotFound, "async search result not found")
	}

	if fracsDone != 0 {
		pr.Progress = float64(fracsDone+fracsInQueue) / float64(fracsDone)
	}
	if pr.Status == fracmanager.AsyncSearchStatusDone {
		pr.Progress = 1
	}
	pr.AggResult = pr.QPR.Aggregate(aggQueries)
	pr.Request = *searchReq

	docsStream := DocsIterator(EmptyDocsStream{})
	var size int
	pr.QPR.IDs, size = paginateIDs(pr.QPR.IDs, r.Offset, r.Size)
	if size > 0 {
		fieldsFilter := tryParseFieldsFilter(pr.Request.Query)
		var err error
		docsStream, err = si.FetchDocsStream(ctx, pr.QPR.IDs, false, fieldsFilter)
		if err != nil {
			return pr, nil, err
		}
	}

	return pr, docsStream, nil
}

func (si *Ingestor) GetAsyncSearchesList(
	ctx context.Context,
	r GetAsyncSearchesListRequest,
) ([]*AsyncSearchesListItem, error) {
	searchStores, err := si.getAsyncSearchStores()
	if err != nil {
		return nil, err
	}

	var searchStatus *storeapi.AsyncSearchStatus
	if r.Status != nil {
		s := storeapi.MustProtoAsyncSearchStatus(*r.Status)
		searchStatus = &s
	}
	req := storeapi.GetAsyncSearchesListRequest{
		Status: searchStatus,
		Size:   int32(r.Size),
		Offset: int32(r.Offset),
	}

	// TODO: handle multiple stores: errors, total disk usage, fracs stats, started at, expired at, etc...
	// TODO: merge info for each of searches

	searches := make([]*AsyncSearchesListItem, 0)

	for _, shard := range searchStores.Shards {
		for _, replica := range shard {
			storeResp, err := si.clients[replica].GetAsyncSearchesList(ctx, &req)
			if err != nil {
				return nil, err
			}

			for _, s := range storeResp.Searches {
				var progress float64
				if s.FracsDone != 0 {
					progress = float64(s.FracsDone+s.FracsQueue) / float64(s.FracsDone)
				}
				if s.Status.MustAsyncSearchStatus() == fracmanager.AsyncSearchStatusDone {
					progress = 1
				}

				reqAggs := make([]AggQuery, 0, len(s.Aggs))
				for _, agg := range s.Aggs {
					reqAggs = append(reqAggs, AggQuery{
						Field:     agg.Field,
						GroupBy:   agg.GroupBy,
						Func:      agg.Func.MustAggFunc(),
						Quantiles: agg.Quantiles,
					})
				}

				var canceledAt time.Time
				if s.CanceledAt != nil {
					canceledAt = s.CanceledAt.AsTime()
				}

				searches = append(searches, &AsyncSearchesListItem{
					ID:         s.SearchId,
					Status:     s.Status.MustAsyncSearchStatus(),
					StartedAt:  s.StartedAt.AsTime(),
					ExpiresAt:  s.ExpiresAt.AsTime(),
					CanceledAt: canceledAt,
					Progress:   progress,
					DiskUsage:  s.DiskUsage,
					Request: AsyncRequest{
						Aggregations:      reqAggs,
						HistogramInterval: seq.MID(s.HistogramInterval),
						Query:             s.Query,
						From:              s.From.AsTime(),
						To:                s.To.AsTime(),
						Retention:         s.Retention.AsDuration(),
						WithDocs:          s.WithDocs,
					},
				})
			}
		}
	}

	return searches, nil
}

func mergeAsyncSearchStatus(a, b fracmanager.AsyncSearchStatus) fracmanager.AsyncSearchStatus {
	statusWeight := []fracmanager.AsyncSearchStatus{
		fracmanager.AsyncSearchStatusDone:       1,
		fracmanager.AsyncSearchStatusInProgress: 2,
		fracmanager.AsyncSearchStatusCanceled:   3,
		fracmanager.AsyncSearchStatusError:      4,
	}
	weightA := statusWeight[a]
	weightB := statusWeight[b]
	if weightA >= weightB {
		return a
	}
	return b
}

func (si *Ingestor) CancelAsyncSearch(ctx context.Context, id string) error {
	searchStores, err := si.getAsyncSearchStores()
	if err != nil {
		return err
	}

	var lastErr error
	cancelSearch := func(client storeapi.StoreApiClient) error {
		_, err := client.CancelAsyncSearch(ctx, &storeapi.CancelAsyncSearchRequest{SearchId: id})
		if err != nil {
			logger.Error("can't cancel async search", zap.String("id", id), zap.Error(err))
			lastErr = err
		}
		return nil
	}

	if err := si.visitEachReplica(searchStores, cancelSearch); err != nil {
		panic(fmt.Errorf("BUG: unexpected error from visit func"))
	}
	if lastErr != nil {
		return fmt.Errorf("unable to cancel async search for all shards in cluster; last err: %w", lastErr)
	}
	return nil
}

func (si *Ingestor) DeleteAsyncSearch(ctx context.Context, id string) error {
	searchStores, err := si.getAsyncSearchStores()
	if err != nil {
		return err
	}

	var lastErr error
	cancelSearch := func(client storeapi.StoreApiClient) error {
		_, err := client.DeleteAsyncSearch(ctx, &storeapi.DeleteAsyncSearchRequest{SearchId: id})
		if err != nil {
			logger.Error("can't delete async search", zap.String("id", id), zap.Error(err))
			lastErr = err
		}
		return nil
	}

	if err := si.visitEachReplica(searchStores, cancelSearch); err != nil {
		panic(fmt.Errorf("BUG: unexpected error from visit func"))
	}
	if lastErr != nil {
		return fmt.Errorf("unable to delete async search for all shards in cluster; last err: %w", lastErr)
	}
	return nil
}

func (si *Ingestor) visitEachReplica(s *stores.Stores, cb func(client storeapi.StoreApiClient) error) error {
	for _, shard := range s.Shards {
		for _, replica := range shard {
			client := si.clients[replica]
			if err := cb(client); err != nil {
				return err
			}
		}
	}
	return nil
}

func (si *Ingestor) getAsyncSearchStores() (*stores.Stores, error) {
	var searchStores *stores.Stores
	// TODO: should we support QueryWantsOldData?
	rs := si.config.ReadStores
	hrs := si.config.HotReadStores
	hs := si.config.HotStores
	if rs != nil && len(rs.Shards) != 0 {
		searchStores = rs
	} else if hrs != nil && len(hrs.Shards) != 0 {
		searchStores = hrs
	} else if hs != nil && len(hs.Shards) != 0 {
		searchStores = si.config.HotStores
	} else {
		return nil, fmt.Errorf("can't find store shards in config")
	}
	return searchStores, nil
}
