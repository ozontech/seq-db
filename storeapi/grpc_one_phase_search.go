package storeapi

import (
	"context"
	"fmt"
	"time"

	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/exec"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func (g *GrpcV1) OnePhaseSearch(
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
	if !g.config.OnePhaseSearch.Enabled {
		err := stream.Send(&storeapi.OnePhaseSearchResponse{
			ResponseType: &storeapi.OnePhaseSearchResponse_Header{
				Header: &storeapi.Header{
					Metadata: &storeapi.Metadata{
						Code: storeapi.SearchErrorCode_DISABLED,
					},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("error sending header: %w", err)
		}
		return nil
	}

	ctx, span := tracing.StartSpan(stream.Context(), "store-server/OnePhaseSearch")
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("request", req.Query))
		span.AddAttributes(trace.StringAttribute("from", req.From.AsTime().Format(time.RFC3339Nano)))
		span.AddAttributes(trace.StringAttribute("to", req.To.AsTime().Format(time.RFC3339Nano)))
		span.AddAttributes(trace.Int64Attribute("size", req.Size))
		span.AddAttributes(trace.Int64Attribute("offset", req.Offset))
		span.AddAttributes(trace.StringAttribute("offset_id", req.OffsetId))
		span.AddAttributes(trace.BoolAttribute("explain", req.Explain))
		span.AddAttributes(trace.BoolAttribute("with_total", req.WithTotal))
	}

	err := g.doOnePhaseSearch(ctx, req, stream)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		logger.Error("one phase search error", zap.Error(err))
	}
	return err
}

func (g *GrpcV1) doOnePhaseSearch(
	ctx context.Context,
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
	metric.SearchInFlightQueriesTotal.Inc()
	defer metric.SearchInFlightQueriesTotal.Dec()

	inflightRequests := g.searchData.inflight.Inc()
	defer g.searchData.inflight.Dec()

	if inflightRequests > int64(g.config.Search.RequestsLimit) {
		metric.RejectedRequests.WithLabelValues("search", "limit_exceeding").Inc()
		return fmt.Errorf("too many search requests: %d > %d", inflightRequests, g.config.Search.RequestsLimit)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	var errCode storeapi.SearchErrorCode
	// in store mode hot we return error in case request wants data, that we've already rotated
	if g.config.StoreMode == StoreModeHot {
		if g.fracManager.Flags().IsCapacityExceeded() && g.earlierThanOldestFrac(uint64(seq.TimeToMID(req.From.AsTime()))) {
			metric.RejectedRequests.WithLabelValues("search", "old_data").Inc()
			errCode = storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA
		}
	}

	tr := querytracer.New(req.Explain, "store/OnePhaseSearchDocs")

	seqql, err := parser.ParseSeqQL(req.Query, g.mappingProvider.GetMapping())
	if err != nil {
		return fmt.Errorf("parse query error: %w", err)
	}

	statsAggs := extractStatsPipesFromSeqQL(seqql)

	// TODO: conditional typing
	typing := []*storeapi.Typing{
		{Title: "mid", Type: storeapi.DataType_UINT64},
		{Title: "rid", Type: storeapi.DataType_UINT64},
		{Title: "data", Type: storeapi.DataType_RAW_DOCUMENT},
	}
	if len(statsAggs) > 0 {
		typing = []*storeapi.Typing{
			{Title: "token", Type: storeapi.DataType_STRING},
			{Title: "min", Type: storeapi.DataType_FLOAT64},
			{Title: "max", Type: storeapi.DataType_FLOAT64},
			{Title: "sum", Type: storeapi.DataType_FLOAT64},
			{Title: "total", Type: storeapi.DataType_UINT64},
			{Title: "not_exists", Type: storeapi.DataType_UINT64},
		}
	}

	err = stream.Send(&storeapi.OnePhaseSearchResponse{
		ResponseType: &storeapi.OnePhaseSearchResponse_Header{
			Header: &storeapi.Header{
				Metadata: &storeapi.Metadata{
					// TODO: fill metadata
					Code: errCode,
				},
				Typing: typing,
			},
		},
	})
	if err != nil {
		if util.IsCancelled(ctx) {
			logger.Info("one phase search request is canceled")
			return nil
		}
		return fmt.Errorf("error sending header: %w", err)
	}

	// exit if had errors
	if errCode != storeapi.SearchErrorCode_NO_ERROR {
		return nil
	}

	producer, err := g.buildProducer(ctx, req, tr, seqql, statsAggs)
	if err != nil {
		return fmt.Errorf("can't biuld record producer: %w", err)
	}

	defer producer.Release()

	curRecord, curMeta := producer.Next()
	for {
		if curMeta != nil && curMeta.Err != nil {
			return fmt.Errorf("producer.Next() error: %w", err)
		}
		if curRecord == nil {
			break
		}

		rawData := make([][]byte, len(curRecord.Vals))
		for i, d := range curRecord.Vals {
			rawData[i] = d.RawData()
		}

		err = stream.Send(&storeapi.OnePhaseSearchResponse{
			ResponseType: &storeapi.OnePhaseSearchResponse_Batch{
				Batch: &storeapi.RecordsBatch{
					Records: []*storeapi.Record{{RawData: rawData}}, // TODO: batch
				},
			},
		})
		if err != nil {
			if util.IsCancelled(ctx) {
				logger.Info("one phase search request is canceled")
				return nil
			}
			return fmt.Errorf("error sending fetched docs: %w", err)
		}

		curRecord, curMeta = producer.Next()
	}

	metric.SearchDurationSeconds.Observe(time.Since(start).Seconds())

	return nil
}

func (g *GrpcV1) buildProducer(
	ctx context.Context,
	req *storeapi.OnePhaseSearchRequest,
	tr *querytracer.Tracer,
	seqql parser.SeqQLQuery,
	statsAggs []parser.StatsAgg,
) (query.RecordProducer, error) {
	searchParams := processor.SearchParams{
		AST:       seqql.Root,
		From:      seq.MillisToMID(uint64(seq.TimeToMID(req.From.AsTime()))),
		To:        seq.MillisToMID(uint64(seq.TimeToMID(req.To.AsTime()))),
		Limit:     int(req.Size + req.Offset),
		WithTotal: req.WithTotal,
		Order:     req.Order.MustDocsOrder(),
	}

	if len(statsAggs) > 0 {
		// TODO: rewrite aggs parsing and validation
		aggs, err := convertStatsAggsToStoreApiAgg(statsAggs)
		if err != nil {
			return nil, fmt.Errorf("failed to convert stats aggs: %w", err)
		}
		aggQ, err := aggQueriesFromProto(aggs)
		if err != nil {
			return nil, fmt.Errorf("failed to convert stats aggs: %w", err)
		}
		searchParams.AggQ = aggQ
	}

	var producer query.RecordProducer

	dataSource := exec.NewSearcherDataSource(ctx, tr, searchParams, g.fracManager, g.searchData.searcher, g.fetchData.docFetcher)
	producer = dataSource

	if len(statsAggs) > 0 {
		// TODO: combine aggs with another executors
		return producer, nil
	}

	for _, pipe := range seqql.Pipes {
		switch p := pipe.(type) {
		case *parser.PipeFilter:
			producer = exec.NewFilter(producer, 2, exec.NewDocFilter(p.Condition.Field, exec.NewEq(p.Condition.Value)))
		case *parser.PipeSort:
			order := exec.OrderAsc
			if p.Order == "desc" {
				order = exec.OrderDesc
			}
			producer = exec.NewDocSorter(producer, 2, p.Field, order)
		case *parser.PipeLimit:
			producer = exec.NewLimiter(producer, uint32(p.Limit))
		default:
			continue
		}
	}

	if req.FieldsFilter != nil && len(req.FieldsFilter.Fields) > 0 {
		producer = exec.NewDocProjector(producer, 2, req.FieldsFilter)
	}

	return producer, nil
}

func extractStatsPipesFromSeqQL(seqql parser.SeqQLQuery) []parser.StatsAgg {
	var result []parser.StatsAgg
	for _, pipe := range seqql.Pipes {
		statsPipe, ok := pipe.(*parser.PipeStats)
		if !ok {
			continue
		}
		result = append(result, statsPipe.Aggs...)
	}
	return result
}

func convertStatsAggsToStoreApiAgg(statsAggs []parser.StatsAgg) ([]*storeapi.AggQuery, error) {
	result := make([]*storeapi.AggQuery, 0, len(statsAggs))

	for _, agg := range statsAggs {
		aggFunc, err := convertStringToAggFunc(agg.Func)
		if err != nil {
			return nil, err
		}

		procAgg := &storeapi.AggQuery{
			Field:     agg.Field,
			GroupBy:   agg.GroupBy,
			Func:      aggFunc,
			Quantiles: agg.Quantiles,
		}

		if agg.Interval != "" {
			interval, err := util.ParseDuration(agg.Interval)
			if err != nil {
				return nil, fmt.Errorf("failed to parse interval: %w", err)
			}
			procAgg.Interval = interval.Nanoseconds()
		}

		result = append(result, procAgg)
	}

	return result, nil
}

func convertStringToAggFunc(funcName string) (storeapi.AggFunc, error) {
	switch funcName {
	case "count":
		return storeapi.AggFunc_AGG_FUNC_COUNT, nil
	case "sum":
		return storeapi.AggFunc_AGG_FUNC_SUM, nil
	case "min":
		return storeapi.AggFunc_AGG_FUNC_MIN, nil
	case "max":
		return storeapi.AggFunc_AGG_FUNC_MAX, nil
	case "avg":
		return storeapi.AggFunc_AGG_FUNC_AVG, nil
	case "quantile":
		return storeapi.AggFunc_AGG_FUNC_QUANTILE, nil
	case "unique":
		return storeapi.AggFunc_AGG_FUNC_UNIQUE, nil
	case "unique_count":
		return storeapi.AggFunc_AGG_FUNC_UNIQUE_COUNT, nil
	default:
		return 0, fmt.Errorf("unknown aggregation function: %s", funcName)
	}
}
