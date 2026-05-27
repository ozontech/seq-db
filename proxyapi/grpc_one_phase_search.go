package proxyapi

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) OnePhaseSearch(ctx context.Context, req *seqproxyapi.SearchRequest) (*seqproxyapi.ComplexSearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	if req.Size <= 0 {
		return nil, status.Error(codes.InvalidArgument, `"size" must be greater than 0`)
	}

	statsAggs := extractStatsPipesFromQuery(req.Query.Query)
	hasAggs := len(statsAggs) > 0

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query:     req.Query,
		Size:      req.Size,
		Offset:    req.Offset,
		OffsetId:  req.OffsetId,
		WithTotal: req.WithTotal,
		Order:     req.Order,
	}

	if hasAggs {
		proxyReq.Aggs = []*seqproxyapi.AggQuery{
			{
				Field:   statsAggs[0].Field,
				GroupBy: statsAggs[0].GroupBy,
				Func:    mustConvertStringToAggFunc(statsAggs[0].Func),
			},
		}
	}

	sResp, stream, err := g.doOnePhaseSearch(ctx, proxyReq, true)
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

	if hasAggs {
		if resp.Aggs, err = makeProtoAggsOnePhase(stream); err != nil {
			return nil, err
		}
	} else {
		if resp.Docs, err = makeProtoDocsOnePhase(stream); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func makeProtoAggsOnePhase(input query.RecordProducer) ([]*seqproxyapi.Aggregation, error) {
	respAggs := []*seqproxyapi.Aggregation{
		{Buckets: make([]*seqproxyapi.Aggregation_Bucket, 0)},
	}

	for {
		r, meta := input.Next()
		if meta != nil {
			if !errors.Is(meta.Err, io.EOF) {
				return nil, fmt.Errorf("read aggs stream error: %w", meta.Err)
			}
			break
		}
		if r == nil {
			break
		}

		respAggs[0].Buckets = append(respAggs[0].Buckets, &seqproxyapi.Aggregation_Bucket{
			Key:   r.Vals[0].Decoded().(string),
			Value: r.Vals[1].Decoded().(float64),
		})
	}

	return respAggs, nil
}

func makeProtoDocsOnePhase(input query.RecordProducer) ([]*seqproxyapi.Document, error) {
	respDocs := make([]*seqproxyapi.Document, 0)

	for {
		r, meta := input.Next()
		if meta != nil {
			if !errors.Is(meta.Err, io.EOF) {
				return nil, fmt.Errorf("read docs stream error: %w", meta.Err)
			}
			break
		}
		if r == nil {
			break
		}

		docID := seq.ID{
			MID: seq.MID(r.Vals[0].Decoded().(uint64)),
			RID: seq.RID(r.Vals[1].Decoded().(uint64)),
		}
		respDocs = append(respDocs, &seqproxyapi.Document{
			Id:   docID.String(),
			Data: r.Vals[2].RawData(),
			Time: timestamppb.New(docID.MID.Time()),
		})
	}

	return respDocs, nil
}

func (g *grpcV1) doOnePhaseSearch(
	ctx context.Context,
	req *seqproxyapi.ComplexSearchRequest,
	shouldFetch bool,
) (*proxySearchResponse, query.RecordProducer, error) {
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

	if len(req.Aggs) > 0 {
		aggs, err := convertAggsQuery(req.Aggs)
		if err != nil {
			return nil, nil, err
		}
		proxyReq.AggQ = aggs
	}

	tr := querytracer.New(req.Query.Explain, "proxy/OnePhaseSearch")
	// TODO: do we really need QPR here (???)
	qpr, stream, err := g.searchIngestor.OnePhaseSearch(ctx, proxyReq, tr)
	psr := &proxySearchResponse{
		qpr: qpr,
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
		return psr, stream, nil
	}
	if err = processSearchErrors(qpr, err); err != nil {
		metric.SearchErrors.Inc()
		return nil, nil, err
	}

	g.tryMirrorRequest(req)

	return psr, stream, nil
}

func extractStatsPipesFromQuery(q string) []parser.StatsAgg {
	if q == "" {
		return nil
	}

	seqql, err := parser.ParseSeqQL(q, nil)
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

func mustConvertStringToAggFunc(funcName string) seqproxyapi.AggFunc {
	switch funcName {
	case "count":
		return seqproxyapi.AggFunc_AGG_FUNC_COUNT
	case "sum":
		return seqproxyapi.AggFunc_AGG_FUNC_SUM
	case "min":
		return seqproxyapi.AggFunc_AGG_FUNC_MIN
	case "max":
		return seqproxyapi.AggFunc_AGG_FUNC_MAX
	case "avg":
		return seqproxyapi.AggFunc_AGG_FUNC_AVG
	case "quantile":
		return seqproxyapi.AggFunc_AGG_FUNC_QUANTILE
	case "unique":
		return seqproxyapi.AggFunc_AGG_FUNC_UNIQUE
	case "unique_count":
		return seqproxyapi.AggFunc_AGG_FUNC_UNIQUE_COUNT
	default:
		panic(fmt.Errorf("unknown aggregation function: %s", funcName))
	}
}
