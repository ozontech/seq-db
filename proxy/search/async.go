package search

import (
	"context"
	"fmt"
	"sort"
	"sync"
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
	"github.com/ozontech/seq-db/util"
)

type AsyncRequest struct {
	Retention         time.Duration
	Query             string
	From              time.Time
	To                time.Time
	Aggregations      []AggQuery
	HistogramInterval seq.MID
	WithDocs          bool
	Size              int64
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
		Size:              r.Size,
	}

	for i, shard := range searchStores.Shards {
		var err error

		idx := util.IdxShuffle(len(shard))
		for i := range len(shard) {
			replica := shard[idx[i]]
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
	IDs    []string
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

	storesCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type shardResponse struct {
		replica string
		data    *storeapi.FetchAsyncSearchResultResponse
		err     error
	}

	wg := sync.WaitGroup{}
	wg.Add(len(searchStores.Shards))
	respChan := make(chan shardResponse, len(searchStores.Shards))
	for _, shard := range searchStores.Shards {
		go func(shard []string) {
			defer wg.Done()

			for _, replica := range shard {
				storeResp, err := si.clients[replica].FetchAsyncSearchResult(storesCtx, &req)
				if err != nil {
					if status.Code(err) == codes.NotFound {
						continue
					}
				}

				respChan <- shardResponse{
					replica: replica,
					data:    storeResp,
					err:     err,
				}

				break
			}
		}(shard)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

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

	for resp := range respChan {
		if err := resp.err; err != nil {
			return FetchAsyncSearchResultResponse{}, nil, err
		}

		anyResponse = true
		storeResp := resp.data
		mergeStoreResp(storeResp, resp.replica)

		if len(aggQueries) == 0 {
			for _, agg := range storeResp.Aggs {
				aggQueries = append(aggQueries, seq.AggregateArgs{
					Func:      agg.Func.MustAggFunc(),
					Quantiles: agg.Quantiles,
				})
			}
		}

		if searchReq == nil {
			searchReq = &AsyncRequest{
				Retention:         storeResp.Retention.AsDuration(),
				Query:             storeResp.Query,
				From:              storeResp.From.AsTime(),
				To:                storeResp.To.AsTime(),
				Aggregations:      buildRequestAggs(storeResp.Aggs),
				HistogramInterval: histInterval,
				WithDocs:          storeResp.WithDocs,
				Size:              storeResp.Size,
			}
		}
	}

	if !anyResponse {
		return FetchAsyncSearchResultResponse{}, nil, status.Error(codes.NotFound, "async search result not found")
	}

	if fracsDone != 0 {
		pr.Progress = float64(fracsDone) / float64(fracsDone+fracsInQueue)
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
		Ids:    r.IDs,
	}

	storesCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type shardResponse struct {
		data *storeapi.GetAsyncSearchesListResponse
		err  error
	}

	wg := sync.WaitGroup{}
	wg.Add(len(searchStores.Shards))
	respChan := make(chan shardResponse, len(searchStores.Shards))
	for _, shard := range searchStores.Shards {
		go func(shard []string) {
			defer wg.Done()

			// we must query all replicas since the required data’s location is unknown in advance
			for _, replica := range shard {
				storeResp, err := si.clients[replica].GetAsyncSearchesList(storesCtx, &req)
				if err != nil {
					if status.Code(err) == codes.NotFound {
						continue
					}
				}

				respChan <- shardResponse{
					data: storeResp,
					err:  err,
				}
			}
		}(shard)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	responsesByID := make(map[string][]*storeapi.AsyncSearchesListItem)

	for resp := range respChan {
		if err := resp.err; err != nil {
			return nil, err
		}

		for _, s := range resp.data.Searches {
			responsesByID[s.SearchId] = append(responsesByID[s.SearchId], s)
		}
	}

	searches := make([]*AsyncSearchesListItem, 0)

	for id, items := range responsesByID {
		fracsDone := 0
		fracsInQueue := 0
		var searchReq *AsyncRequest
		search := AsyncSearchesListItem{
			ID: id,
		}

		mergeListItem := func(sr *storeapi.AsyncSearchesListItem) {
			search.DiskUsage += sr.DiskUsage
			fracsInQueue += int(sr.FracsQueue)
			fracsDone += int(sr.FracsDone)

			ss := sr.Status.MustAsyncSearchStatus()
			search.Status = mergeAsyncSearchStatus(search.Status, ss)

			t := sr.StartedAt.AsTime()
			if search.StartedAt.IsZero() || search.StartedAt.After(t) {
				search.StartedAt = t
			}
			t = sr.ExpiresAt.AsTime()
			if search.ExpiresAt.IsZero() || search.ExpiresAt.After(t) {
				search.ExpiresAt = t
			}
			t = sr.CanceledAt.AsTime()
			if sr.CanceledAt != nil && (search.CanceledAt.IsZero() || search.CanceledAt.After(t)) {
				search.CanceledAt = t
			}
		}

		for _, s := range items {
			mergeListItem(s)

			if searchReq == nil {
				searchReq = &AsyncRequest{
					Retention:         s.Retention.AsDuration(),
					Query:             s.Query,
					From:              s.From.AsTime(),
					To:                s.To.AsTime(),
					Aggregations:      buildRequestAggs(s.Aggs),
					HistogramInterval: seq.MID(s.HistogramInterval),
					WithDocs:          s.WithDocs,
					Size:              s.Size,
				}
			}
		}

		if fracsDone != 0 {
			search.Progress = float64(fracsDone) / float64(fracsDone+fracsInQueue)
		}
		if search.Status == fracmanager.AsyncSearchStatusDone {
			search.Progress = 1
		}
		search.Request = *searchReq

		searches = append(searches, &search)
	}

	// order by StartedAt DESC
	sort.Slice(searches, func(i, j int) bool {
		return searches[i].StartedAt.After(searches[j].StartedAt)
	})

	// limit offset
	if r.Offset > 0 {
		searches = searches[min(r.Offset, len(searches)):]
	}
	if r.Size > 0 {
		searches = searches[:min(r.Size, len(searches))]
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

func buildRequestAggs(in []*storeapi.AggQuery) []AggQuery {
	reqAggs := make([]AggQuery, 0, len(in))
	for _, agg := range in {
		reqAggs = append(reqAggs, AggQuery{
			Field:     agg.Field,
			GroupBy:   agg.GroupBy,
			Func:      agg.Func.MustAggFunc(),
			Quantiles: agg.Quantiles,
		})
	}
	return reqAggs
}

func (si *Ingestor) CancelAsyncSearch(ctx context.Context, id string) error {
	searchStores, err := si.getAsyncSearchStores()
	if err != nil {
		return err
	}

	var lastErr error
	cancelSearch := func(client storeapi.StoreApiClient) {
		_, err := client.CancelAsyncSearch(ctx, &storeapi.CancelAsyncSearchRequest{SearchId: id})
		if err != nil {
			logger.Error("can't cancel async search", zap.String("id", id), zap.Error(err))
			lastErr = err
		}
	}

	si.visitEachReplica(searchStores, cancelSearch)
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
	cancelSearch := func(client storeapi.StoreApiClient) {
		_, err := client.DeleteAsyncSearch(ctx, &storeapi.DeleteAsyncSearchRequest{SearchId: id})
		if err != nil {
			logger.Error("can't delete async search", zap.String("id", id), zap.Error(err))
			lastErr = err
		}
	}

	si.visitEachReplica(searchStores, cancelSearch)
	if lastErr != nil {
		return fmt.Errorf("unable to delete async search for all shards in cluster; last err: %w", lastErr)
	}
	return nil
}

func (si *Ingestor) visitEachReplica(s *stores.Stores, cb func(client storeapi.StoreApiClient)) {
	for _, shard := range s.Shards {
		for _, replica := range shard {
			client := si.clients[replica]
			cb(client)
		}
	}
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
		searchStores = hs
	} else {
		return nil, fmt.Errorf("can't find store shards in config")
	}
	return searchStores, nil
}
