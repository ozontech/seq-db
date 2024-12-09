package proxyapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
)

func (g *grpcV1) GetAggregation(
	ctx context.Context, req *seqproxyapi.GetAggregationRequest,
) (*seqproxyapi.GetAggregationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	if req.Aggs == nil {
		return nil, status.Error(codes.InvalidArgument, "agg query must be provided")
	}

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query: req.Query,
		Aggs:  req.Aggs,
	}
	sResp, err := g.doSearch(ctx, proxyReq, false)
	if err != nil {
		return nil, err
	}
	if sResp.err != nil && !shouldHaveResponse(sResp.err.Code) {
		return &seqproxyapi.GetAggregationResponse{Error: sResp.err}, nil
	}

	allAggregations := sResp.qpr.Aggregate(aggregationArgsFromProto(req.Aggs))
	resp := &seqproxyapi.GetAggregationResponse{
		Aggs:  makeProtoAggregation(allAggregations),
		Total: int64(sResp.qpr.Total),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}
	if sResp.err != nil {
		resp.Error = sResp.err
		resp.PartialResponse = sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE
	}

	return resp, nil
}
