package proxyapi

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) ComplexSearch(
	ctx context.Context, req *seqproxyapi.ComplexSearchRequest,
) (_ *seqproxyapi.ComplexSearchResponse, retErr error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	if req.Size <= 0 && req.Hist == nil && len(req.Aggs) == 0 {
		return nil, status.Error(codes.InvalidArgument, `one of "size", "hist" or "aggs" must be provided`)
	}

	tr := querytracer.New(req.Query.Explain, "proxy/ComplexSearch")

	if shouldTryStreamSearch(g.config.TryStreamSearch, req) {
		return g.emulateStreamSearch(ctx, req, tr)
	}

	sResp, obs, err := g.doSearch(ctx, req, true, true, tr)
	defer func() { obs.finish("ComplexSearch", retErr) }()
	if err != nil {
		return nil, err
	}
	if sResp.err != nil && sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE && shouldFailPartialResponse(ctx) {
		return nil, status.Error(codes.Internal, "partial response: not all shards returned results")
	}
	if sResp.err != nil && !shouldHaveResponse(sResp.err.Code) {
		return &seqproxyapi.ComplexSearchResponse{Error: sResp.err}, nil
	}

	resp := &seqproxyapi.ComplexSearchResponse{
		Docs:  makeProtoDocs(sResp.qpr, sResp.docsStream),
		Total: int64(sResp.qpr.Total),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}
	if req.Aggs != nil {
		aggTr := tr.NewChild("aggregate")
		allAggregations := sResp.qpr.Aggregate(aggregationArgsFromProto(req.Aggs))
		resp.Aggs = makeProtoAggregation(allAggregations)
		aggTr.Done()
	}
	if req.Hist != nil {
		histTr := tr.NewChild("histogram")
		resp.Hist = makeProtoHistogram(sResp.qpr)
		histTr.Done()
	}
	if sResp.err != nil {
		resp.Error = sResp.err
		resp.PartialResponse = sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE
	}

	tr.Done()
	resp.Explain = tracerSpanToExplainEntry(tr.ToSpan())

	return resp, nil
}

func aggregationArgsFromProto(aggs []*seqproxyapi.AggQuery) []seq.AggregateArgs {
	args := make([]seq.AggregateArgs, len(aggs))
	for i, agg := range aggs {
		args[i] = seq.AggregateArgs{
			Func:                 agg.Func.MustAggFunc(),
			Quantiles:            agg.Quantiles,
			SkipWithoutTimestamp: agg.Interval != nil,
		}
	}
	return args
}

func (g *grpcV1) emulateStreamSearch(
	ctx context.Context,
	req *seqproxyapi.ComplexSearchRequest,
	tr *querytracer.Tracer,
) (*seqproxyapi.ComplexSearchResponse, error) {
	metric.SearchOverall.Add(1)
	tr.Printf("making stream search request")

	streamSearchReq, err := buildStreamSearchReqFromComplexSearchReq(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, `can't build stream search request`)
	}

	var partialErr error
	storesStream, broadcaster, err := g.searchIngestor.StreamSearch(ctx, streamSearchReq, tr)
	if err != nil {
		// The stores were not opened or failed to open, cancel any that may have started before propagating the error.
		if broadcaster != nil {
			broadcaster.SendControl(storeapi.ControlAction_CANCEL)
		}
		if errors.Is(err, consts.ErrPartialResponse) {
			if shouldFailPartialResponse(ctx) {
				return nil, status.Error(codes.Internal, "partial response: not all shards returned results")
			}
			partialErr = err
			metric.SearchPartial.Inc()
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	var docs []*seqproxyapi.Document
	var aggs []*seqproxyapi.Aggregation
	if streamSearchReq.Agg != nil {
		aggs = readAggregations(storesStream)
	} else {
		docs = readDocuments(storesStream)
	}

	// finalize to get summary
	broadcaster.SendControl(storeapi.ControlAction_FINALIZE)

	summary := storesStream.Finalize()
	if summary == nil {
		summary = &query.Summary{}
	}
	if partialErr != nil && summary.Err == nil {
		summary.Err = partialErr
	}

	if req.Query.Explain {
		tr.Done()
	}

	resp := &seqproxyapi.ComplexSearchResponse{
		Total:   int64(summary.Total),
		Docs:    docs,
		Aggs:    aggs,
		Explain: tracerSpanToExplainEntry(tr.ToSpan()),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}
	if summary.Err != nil {
		resp.Error = &seqproxyapi.Error{
			Code:    mapProxyErrorCode(summary.Err),
			Message: summary.Err.Error(),
		}
		if !shouldHaveResponse(resp.Error.Code) {
			resp.Docs = nil
			resp.Aggs = nil
		}
	}

	return resp, nil
}

func readDocuments(storesStream query.RecordProducer) []*seqproxyapi.Document {
	var docs []*seqproxyapi.Document
	for r := storesStream.Next(); r != nil; r = storesStream.Next() {
		id := r.Vals[0].Decoded().(seq.ID)
		docs = append(docs, &seqproxyapi.Document{
			Id:   id.String(),
			Time: timestamppb.New(id.MID.Time()),
			Data: r.Vals[1].RawData(),
		})
	}
	return docs
}

func readAggregations(storesStream query.RecordProducer) []*seqproxyapi.Aggregation {
	buckets := make([]*seqproxyapi.Aggregation_Bucket, 0)
	for r := storesStream.Next(); r != nil; r = storesStream.Next() {
		bucket := &seqproxyapi.Aggregation_Bucket{
			Key:   r.Vals[0].Decoded().(string),
			Value: r.Vals[1].Decoded().(float64),
		}
		if ts := r.Vals[2].Decoded().(uint64); ts != consts.DummyMID {
			bucket.Ts = timestamppb.New(seq.MID(ts).Time())
		}
		if quantiles := r.Vals[3].Decoded().([]float64); len(quantiles) > 0 {
			bucket.Quantiles = quantiles
		}
		buckets = append(buckets, bucket)
	}
	return []*seqproxyapi.Aggregation{{Buckets: buckets}}
}

func shouldTryStreamSearch(tryStreamSearch bool, req *seqproxyapi.ComplexSearchRequest) bool {
	if !tryStreamSearch {
		return false
	}
	if req.Hist != nil || len(req.Aggs) > 1 {
		return false
	}
	return true
}

func buildStreamSearchReqFromComplexSearchReq(
	req *seqproxyapi.ComplexSearchRequest,
) (*search.StreamSearchRequest, error) {
	seqql, err := parser.ParseSeqQL(req.Query.Query, nil)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	streamSearchReq := &search.StreamSearchRequest{
		Query:     req.Query.Query,
		From:      seq.TimeToMID(req.Query.From.AsTime()),
		To:        seq.TimeToMID(req.Query.To.AsTime()),
		Explain:   req.Query.Explain,
		WithTotal: req.WithTotal,
		OffsetId:  req.OffsetId,
		Order:     req.Order.MustDocsOrder(),
		Size:      int(req.Size),
		Offset:    int(req.Offset),
	}

	// stream search serves either documents or a single agg.
	// shouldTryStreamSearch guarantees single agg and no histogram.
	if len(req.Aggs) == 1 {
		aggQuery, err := convertAggsQuery(req.Aggs)
		if err != nil {
			return nil, err
		}
		streamSearchReq.Agg = &aggQuery[0]
		seqql.Pipes = append(seqql.Pipes, statsPipeFromProto(req.Aggs[0]))
	} else {
		if req.Size > 0 {
			seqql.Pipes = append(seqql.Pipes, &parser.PipeLimit{Limit: int(req.Size)})
		}
		if req.Offset > 0 {
			seqql.Pipes = append(seqql.Pipes, &parser.PipeOffset{Offset: int(req.Offset)})
		}
		seqql.Pipes = append(seqql.Pipes, &parser.PipeSort{Order: orderToPipeString(req.Order)})
	}

	// pipes order is important
	slices.SortFunc(seqql.Pipes, func(a, b parser.Pipe) int {
		return cmp.Compare(pipeOrder[a.Name()], pipeOrder[b.Name()])
	})

	// we need to modify query with pipes because stores extracts parameters from it.
	streamSearchReq.Query = seqql.SeqQLString()

	return streamSearchReq, nil
}

var pipeOrder = map[string]int{
	"stats":  0,
	"filter": 1,
	"fields": 2,
	"sort":   3,
	"limit":  4,
	"offset": 5,
}

func orderToPipeString(order seqproxyapi.Order) string {
	if order == seqproxyapi.Order_ORDER_ASC {
		return "asc"
	}
	return "desc"
}

func statsPipeFromProto(agg *seqproxyapi.AggQuery) *parser.PipeStats {
	statsAgg := parser.StatsAgg{
		Func:      agg.Func.MustAggFunc().String(),
		Field:     agg.Field,
		GroupBy:   agg.GroupBy,
		Quantiles: agg.Quantiles,
	}
	if agg.Interval != nil {
		statsAgg.Interval = *agg.Interval
	}
	return &parser.PipeStats{Agg: statsAgg}
}
