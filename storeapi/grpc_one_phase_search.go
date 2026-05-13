package storeapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query/exec"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func (g *GrpcV1) OnePhaseSearch(
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
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
	statsAggs := extractStatsPipesFromQuery(req.Query)
	hasAggs := len(statsAggs) > 0

	useNewQueryEngine := true
	if useNewQueryEngine {
		return g.doOnePhaseSearchNewQueryEngine(ctx, req, stream)
	}

	if hasAggs {
		return g.doOnePhaseSearchWithAggs(ctx, req, stream, statsAggs)
	}

	return g.doOnePhaseSearchDocs(ctx, req, stream)
}

func (g *GrpcV1) doOnePhaseSearchNewQueryEngine(
	ctx context.Context,
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
	tr := querytracer.New(req.Explain, "store/OnePhaseSearchDocs")

	ast, err := g.parseQuery(req.Query)
	if err != nil {
		return fmt.Errorf("parseQuery error: %w", err)
	}

	from := seq.MillisToMID(uint64(seq.TimeToMID(req.From.AsTime())))
	to := seq.MillisToMID(uint64(seq.TimeToMID(req.To.AsTime())))

	searchParams := processor.SearchParams{
		AST:       ast,
		From:      from,
		To:        to,
		Limit:     int(req.Size + req.Offset),
		WithTotal: req.WithTotal,
		Order:     req.Order.MustDocsOrder(),
	}

	err = stream.Send(&storeapi.OnePhaseSearchResponse{
		ResponseType: &storeapi.OnePhaseSearchResponse_Header{
			Header: &storeapi.Header{
				Metadata: &storeapi.Metadata{}, // TODO: fill metadata
				Typing: []*storeapi.Typing{
					// TODO: conditional typing
					{Title: "mid", Type: storeapi.DataType_UINT64},
					{Title: "rid", Type: storeapi.DataType_UINT64},
					{Title: "data", Type: storeapi.DataType_RAW_DOCUMENT},
				},
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

	dataSource := exec.NewSearcherDataSource(ctx, tr, searchParams, g.fracManager, g.searchData.searcher, g.fetchData.docFetcher)
	// TODO: configurable executors
	filter := exec.NewFilter(dataSource, 2, exec.NewDocFilter("message", exec.NewEq("dg guillotine 2")))
	limiter := exec.NewLimiter(filter, 5)

	producer := limiter
	curRecord, curMeta := producer.Next()
	for {
		if curMeta != nil && curMeta.Err != nil {
			return fmt.Errorf("producer.Next() error: %w", err)
		}
		if curRecord == nil {
			break
		}

		err = stream.Send(&storeapi.OnePhaseSearchResponse{
			ResponseType: &storeapi.OnePhaseSearchResponse_Batch{
				Batch: &storeapi.RecordsBatch{
					// TODO: batch
					Records: []*storeapi.Record{
						{
							RawData: [][]byte{
								curRecord.Vals[0].RawData(),
								curRecord.Vals[1].RawData(),
								curRecord.Vals[2].RawData(),
							},
						},
					},
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

	return nil
}

func (g *GrpcV1) doOnePhaseSearchDocs(
	ctx context.Context,
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
	tr := querytracer.New(req.Explain, "store/OnePhaseSearchDocs")
	data, err := g.doSearch(ctx, &storeapi.SearchRequest{
		Query:     req.Query,
		From:      int64(seq.TimeToMID(req.From.AsTime())),
		To:        int64(seq.TimeToMID(req.To.AsTime())),
		Size:      req.Size,
		Offset:    req.Offset,
		Explain:   req.Explain,
		WithTotal: req.WithTotal,
		Order:     req.Order,
		OffsetId:  req.OffsetId,
	}, tr)
	if err != nil {
		return fmt.Errorf("search error: %w", err)
	}

	tr.Done()
	if req.Explain && data != nil {
		data.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	}

	err = stream.Send(&storeapi.OnePhaseSearchResponse{
		ResponseType: &storeapi.OnePhaseSearchResponse_Header{
			Header: &storeapi.Header{
				Metadata: &storeapi.Metadata{
					Total:   data.Total,
					Code:    data.Code,
					Errors:  data.Errors,
					Explain: data.Explain,
				},
				Typing: []*storeapi.Typing{
					// TODO: conditional typing
					{Title: "mid", Type: storeapi.DataType_UINT64},
					{Title: "rid", Type: storeapi.DataType_UINT64},
					{Title: "data", Type: storeapi.DataType_RAW_DOCUMENT},
				},
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

	ids := make(seq.IDSources, 0, len(data.IdSources))
	for _, id := range data.IdSources {
		ids = append(ids, seq.IDSource{
			ID:   seq.ID{MID: seq.MID(id.Id.Mid), RID: seq.RID(id.Id.Rid)},
			Hint: id.Hint,
		})
	}

	send := func(block []byte) error {
		// TODO: get rid of hardcode
		docBlock := storage.DocBlock(block)
		return stream.Send(&storeapi.OnePhaseSearchResponse{
			ResponseType: &storeapi.OnePhaseSearchResponse_Batch{
				Batch: &storeapi.RecordsBatch{
					// TODO: batch
					Records: []*storeapi.Record{
						{
							RawData: [][]byte{
								Uint64ToBytes(docBlock.GetExt1()),
								Uint64ToBytes(docBlock.GetExt2()),
								docBlock.Payload(),
							},
						},
					},
				},
			},
		})
	}
	err = g.doFetch(ctx, ids, req.FieldsFilter, req.Explain, send)
	if err != nil {
		return fmt.Errorf("fetch error: %w", err)
	}

	return nil
}

// TODO: bytes pool (???), varint (???)
func Uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func Float64ToBytes(val float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(val))
	return b
}

func (g *GrpcV1) doOnePhaseSearchWithAggs(
	ctx context.Context,
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
	statsAggs []parser.StatsAgg,
) error {
	tr := querytracer.New(req.Explain, "store/OnePhaseSearchWithAggs")

	aggQ, err := convertStatsAggsToStoreApiAgg(statsAggs)
	if err != nil {
		return fmt.Errorf("failed to convert stats aggs: %w", err)
	}

	data, err := g.doSearch(ctx, &storeapi.SearchRequest{
		Query:     req.Query,
		From:      int64(seq.TimeToMID(req.From.AsTime())),
		To:        int64(seq.TimeToMID(req.To.AsTime())),
		Size:      req.Size,
		Offset:    req.Offset,
		Explain:   req.Explain,
		WithTotal: req.WithTotal,
		Order:     req.Order,
		OffsetId:  req.OffsetId,
		Aggs:      aggQ,
	}, tr)
	if err != nil {
		return fmt.Errorf("search error: %w", err)
	}
	tr.Done()

	err = stream.Send(&storeapi.OnePhaseSearchResponse{
		ResponseType: &storeapi.OnePhaseSearchResponse_Header{
			Header: &storeapi.Header{
				Metadata: &storeapi.Metadata{
					Total:   data.Total,
					Code:    data.Code,
					Errors:  data.Errors,
					Explain: data.Explain,
				},
				Typing: []*storeapi.Typing{
					// TODO: conditional typing
					{Title: "token", Type: storeapi.DataType_STRING},
					{Title: "min", Type: storeapi.DataType_FLOAT64},
					{Title: "max", Type: storeapi.DataType_FLOAT64},
					{Title: "sum", Type: storeapi.DataType_FLOAT64},
					{Title: "total", Type: storeapi.DataType_UINT64},
					{Title: "not_exists", Type: storeapi.DataType_UINT64},
				},
			},
		},
	})
	if err != nil {
		if util.IsCancelled(ctx) {
			logger.Info("one phase search request is canceled")
			return nil
		}
		return fmt.Errorf("error sending aggs: %w", err)
	}

	for _, agg := range data.Aggs {
		for _, bin := range agg.Timeseries {
			err := stream.Send(&storeapi.OnePhaseSearchResponse{
				ResponseType: &storeapi.OnePhaseSearchResponse_Batch{
					Batch: &storeapi.RecordsBatch{
						// TODO: batch
						Records: []*storeapi.Record{
							{
								RawData: [][]byte{
									[]byte(bin.Label),
									Float64ToBytes(bin.Hist.Min),
									Float64ToBytes(bin.Hist.Max),
									Float64ToBytes(bin.Hist.Sum),
									Uint64ToBytes(uint64(bin.Hist.Total)),
									Uint64ToBytes(uint64(bin.Hist.NotExists)),
								},
							},
						},
					},
				},
			})
			if err != nil {
				if util.IsCancelled(ctx) {
					logger.Info("one phase search request is canceled")
					return nil
				}
				return fmt.Errorf("error sending aggs: %w", err)
			}
		}
	}

	return nil
}

func extractStatsPipesFromQuery(query string) []parser.StatsAgg {
	if query == "" {
		return nil
	}

	seqql, err := parser.ParseSeqQL(query, nil)
	if err != nil {
		return nil
	}

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
