package proxyapi

import (
	"context"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) ComplexSearch(
	ctx context.Context, req *seqproxyapi.ComplexSearchRequest,
) (*seqproxyapi.ComplexSearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	sResp, err := g.doSearch(ctx, req, true)
	if err != nil {
		return nil, err
	}

	resp := &seqproxyapi.ComplexSearchResponse{
		Docs:  makeProtoDocs(sResp.qpr, sResp.docsStream),
		Total: int64(sResp.qpr.Total),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}
	if req.Aggs != nil {
		allAggregations := sResp.qpr.Aggregate(aggregationArgsFromProto(req.Aggs))
		resp.Aggs = makeProtoAggregation(allAggregations)
	}
	if req.Hist != nil {
		resp.Hist = makeProtoHistogram(sResp.qpr)
	}
	if sResp.err != nil {
		resp.Error = sResp.err
		resp.PartialResponse = sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE
	}

	return resp, nil
}

func aggregationArgsFromProto(aggs []*seqproxyapi.AggQuery) []seq.AggregateArgs {
	args := make([]seq.AggregateArgs, len(aggs))
	for i, agg := range aggs {
		args[i] = seq.AggregateArgs{
			Func:      agg.Func.MustAggFunc(),
			Quantiles: agg.Quantiles,
		}
	}
	return args
}
