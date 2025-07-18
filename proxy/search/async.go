package search

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
)

type AsyncRequest struct {
	Query             string
	From              time.Time
	To                time.Time
	Order             seq.DocsOrder
	Aggregations      []AggQuery
	HistogramInterval seq.MID
}

type AsyncResponse struct {
	ID string
}

func (si *Ingestor) StartAsyncSearch(ctx context.Context, r AsyncRequest) (AsyncResponse, error) {
	requestID := uuid.New().String()

	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	req := storeapi.StartAsyncSearchRequest{
		SearchId:          requestID,
		Query:             r.Query,
		From:              r.From.UnixMilli(),
		To:                r.To.UnixMilli(),
		Aggs:              convertToAggsQuery(r.Aggregations),
		Order:             storeapi.MustProtoOrder(r.Order),
		HistogramInterval: int64(r.HistogramInterval),
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
	ID       string
	WithDocs bool
	Size     int
	Offset   int
}

type FetchAsyncSearchResultResponse struct {
	Done       bool
	Expiration time.Time
	QPR        seq.QPR
	AggResult  []seq.AggregationResult
}

func (si *Ingestor) FetchAsyncSearchResult(ctx context.Context, r FetchAsyncSearchResultRequest) (FetchAsyncSearchResultResponse, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	req := storeapi.FetchAsyncSearchResultRequest{
		SearchId: r.ID,
		WithDocs: r.WithDocs,
		Size:     int32(r.Size),
		Offset:   int32(r.Offset),
	}

	done := true
	var expiration time.Time
	var aggQueries []seq.AggregateArgs

	var qprs []*seq.QPR
	anyResponse := false
	histInterval := seq.MID(0)
	aggsCount := 0
	order := seq.DocsOrderAsc
	for _, shard := range searchStores.Shards {
		var storeResp *storeapi.FetchAsyncSearchResultResponse
		var err error
		var replica string
		for _, replica = range shard {
			storeResp, err = si.clients[replica].FetchAsyncSearchResult(ctx, &req)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					continue
				}
				return FetchAsyncSearchResultResponse{}, err
			}
			break
		}
		if err != nil {
			logger.Warn("shard does not have async search request")
			continue
		}

		anyResponse = true

		histInterval = seq.MID(storeResp.HistogramInterval)
		aggsCount = len(storeResp.Aggs)
		order = storeResp.Order.MustDocsOrder()

		if !storeResp.Done {
			done = false
		}

		storeExpiration := storeResp.Expiration.AsTime()
		if expiration.IsZero() || expiration.After(storeExpiration) {
			expiration = storeExpiration
		}

		for _, agg := range storeResp.Aggs {
			aggQueries = append(aggQueries, seq.AggregateArgs{
				Func:                 agg.Func.MustAggFunc(),
				Quantiles:            agg.Quantiles,
				SkipWithoutTimestamp: agg.Interval > 0,
			})
		}
		qpr := responseToQPR(storeResp.Response, si.sourceByClient[replica], false) // todo pass args
		qprs = append(qprs, qpr)
	}

	if !anyResponse {
		return FetchAsyncSearchResultResponse{}, status.Error(codes.NotFound, "async search result not found")
	}

	qpr := seq.QPR{
		Aggs: make([]seq.AggregatableSamples, aggsCount),
	}
	seq.MergeQPRs(&qpr, qprs, r.Size, histInterval, order)

	aggResult := qpr.Aggregate(aggQueries)

	return FetchAsyncSearchResultResponse{
		Done:       done,
		Expiration: expiration,
		QPR:        qpr,
		AggResult:  aggResult,
	}, nil
}
