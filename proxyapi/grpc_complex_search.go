package proxyapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) ComplexSearch(
	ctx context.Context, req *seqproxyapi.ComplexSearchRequest,
) (*seqproxyapi.ComplexSearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	if req.Size <= 0 && req.Hist == nil && len(req.Aggs) == 0 {
		return nil, status.Error(codes.InvalidArgument, `one of "size", "hist" or "aggs" must be provided`)
	}

	tr := querytracer.New(req.Query.Explain, "proxy/ComplexSearch")
	sResp, err := g.doSearch(ctx, req, true, true, tr)
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
