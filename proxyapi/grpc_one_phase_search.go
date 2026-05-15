package proxyapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) OnePhaseSearch(ctx context.Context, req *seqproxyapi.SearchRequest) (*seqproxyapi.ComplexSearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	if req.Size <= 0 {
		return nil, status.Error(codes.InvalidArgument, `"size" must be greater than 0`)
	}

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query:     req.Query,
		Size:      req.Size,
		Offset:    req.Offset,
		OffsetId:  req.OffsetId,
		WithTotal: req.WithTotal,
		Order:     req.Order,
	}
	sResp, aggsStream, err := g.doOnePhaseSearch(ctx, proxyReq, true)
	if err != nil {
		return nil, err
	}

	resp := &seqproxyapi.ComplexSearchResponse{
		Total: int64(sResp.qpr.Total),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}

	if sResp.err != nil {
		if shouldHaveResponse(sResp.err.Code) {
			resp.Error = sResp.err
		} else {
			return &seqproxyapi.ComplexSearchResponse{Error: sResp.err}, nil
		}
	}

	statsAggs := extractStatsPipesFromQuery(req.Query.Query)
	hasAggs := len(statsAggs) > 0

	if hasAggs {
		sResp.qpr.Aggs = convertAggsStreamToAggregationResults(aggsStream)
		allAggs := sResp.qpr.Aggregate(aggregationArgsFromStatsAggs(statsAggs))
		resp.Aggs = makeProtoAggregation(allAggs)
	} else {
		resp.Docs = makeProtoDocsOnePhase(sResp.docsStream)
	}

	return resp, nil
}

func convertAggsStreamToAggregationResults(aggs search.AggsIterator) []seq.AggregatableSamples {
	result := make([]seq.AggregatableSamples, 0)
	to := make(map[seq.AggBin]*seq.SamplesContainer)
	for agg, err := aggs.Next(); err == nil; agg, err = aggs.Next() {

		tbin := seq.AggBin{
			MID:   consts.DummyMID,
			Token: agg.Label,
		}

		to[tbin] = &seq.SamplesContainer{
			Min:       agg.Min,
			Max:       agg.Max,
			Sum:       agg.Sum,
			Total:     int64(agg.Total),
			NotExists: int64(agg.NotExists),
		}
	}
	result = append(result, seq.AggregatableSamples{
		SamplesByBin: to,
		// NotExists:    int64(agg.NotExists),
	})
	return result
}

func makeProtoDocsOnePhase(docs search.DocsIterator) []*seqproxyapi.Document {
	// TODO: paginate (???)
	respDocs := make([]*seqproxyapi.Document, 0)
	for doc, err := docs.Next(); err == nil; doc, err = docs.Next() {
		respDocs = append(respDocs, &seqproxyapi.Document{
			Id:   doc.ID.String(),
			Data: doc.Data,
			Time: timestamppb.New(doc.ID.MID.Time()),
		})
	}
	return respDocs
}

func (g *grpcV1) doOnePhaseSearch(
	ctx context.Context,
	req *seqproxyapi.ComplexSearchRequest,
	shouldFetch bool,
) (*proxySearchResponse, search.AggsIterator, error) {
	metric.SearchOverall.Add(1)

	span := trace.FromContext(ctx)
	defer span.End()

	if req.Query == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "search query must be provided")
	}
	if req.Query.From == nil || req.Query.To == nil {
		return nil, nil, status.Error(codes.InvalidArgument, `search query "from" and "to" fields must be provided`)
	}
	if req.Offset != 0 && req.OffsetId != "" {
		return nil, nil, status.Error(codes.InvalidArgument, `only one of "offset" and "offset_id" must be provided`)
	}

	fromTime := req.Query.From.AsTime()
	toTime := req.Query.To.AsTime()
	if span.IsRecordingEvents() {
		span.AddAttributes(
			trace.StringAttribute("query", req.Query.Query),
			trace.StringAttribute("from", fromTime.UTC().Format(time.RFC3339Nano)),
			trace.StringAttribute("to", toTime.UTC().Format(time.RFC3339Nano)),
			trace.BoolAttribute("explain", req.Query.Explain),
			trace.Int64Attribute("size", req.Size),
			trace.Int64Attribute("offset", req.Offset),
			trace.StringAttribute("offset_id", req.OffsetId),
			trace.BoolAttribute("with_total", req.WithTotal),
			trace.StringAttribute("order", req.Order.String()),
		)
	}

	rlQuery := getSearchQueryFromGRPCReqForRateLimiter(req)
	if !g.rateLimiter.Account(rlQuery) {
		return nil, nil, status.Error(codes.ResourceExhausted, consts.ErrRequestWasRateLimited.Error())
	}

	proxyReq := &search.SearchRequest{
		Q:           []byte(req.Query.Query),
		From:        seq.MID(fromTime.UnixNano()),
		To:          seq.MID(toTime.UnixNano()),
		Explain:     req.Query.Explain,
		Size:        int(req.Size),
		Offset:      int(req.Offset),
		OffsetId:    req.OffsetId,
		WithTotal:   req.WithTotal,
		ShouldFetch: shouldFetch,
		Order:       req.Order.MustDocsOrder(),
	}

	tr := querytracer.New(req.Query.Explain, "proxy/OnePhaseSearch")
	qpr, docsStream, aggsStream, err := g.searchIngestor.OnePhaseSearch(ctx, proxyReq, tr)
	psr := &proxySearchResponse{
		qpr:        qpr,
		docsStream: docsStream,
	}

	if e, ok := parseProxyError(err); ok {
		psr.err = e
		return psr, nil, nil
	}

	if errors.Is(err, consts.ErrInvalidArgument) {
		return nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if st, ok := status.FromError(err); ok {
		// could not parse a query
		if st.Code() == codes.InvalidArgument {
			return nil, nil, err
		}
	}

	if errors.Is(err, consts.ErrPartialResponse) {
		metric.SearchPartial.Inc()
		psr.err = &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE,
			Message: err.Error(),
		}
		return psr, aggsStream, nil
	}
	if err = processSearchErrors(qpr, err); err != nil {
		metric.SearchErrors.Inc()
		return nil, nil, err
	}

	g.tryMirrorRequest(req)

	return psr, aggsStream, nil
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

func aggregationArgsFromStatsAggs(aggs []parser.StatsAgg) []seq.AggregateArgs {
	args := make([]seq.AggregateArgs, len(aggs))
	for i, agg := range aggs {
		args[i] = seq.AggregateArgs{
			Func:                 mustConvertStringToAggFunc(agg.Func),
			Quantiles:            agg.Quantiles,
			SkipWithoutTimestamp: agg.Interval != "",
		}
	}
	return args
}

func mustConvertStringToAggFunc(funcName string) seq.AggFunc {
	switch funcName {
	case "count":
		return seq.AggFuncCount
	case "sum":
		return seq.AggFuncSum
	case "min":
		return seq.AggFuncMin
	case "max":
		return seq.AggFuncMax
	case "avg":
		return seq.AggFuncAvg
	case "quantile":
		return seq.AggFuncQuantile
	case "unique":
		return seq.AggFuncUnique
	case "unique_count":
		return seq.AggFuncUniqueCount
	default:
		panic(fmt.Errorf("unknown aggregation function: %s", funcName))
	}
}
