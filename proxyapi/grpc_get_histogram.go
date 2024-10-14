package proxyapi

import (
	"context"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (g *grpcV1) GetHistogram(
	ctx context.Context, req *seqproxyapi.GetHistogramRequest,
) (*seqproxyapi.GetHistogramResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	if req.Hist == nil {
		return nil, status.Error(codes.InvalidArgument, "hist query must be provided")
	}

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query: req.Query,
		Hist:  req.Hist,
	}
	sResp, err := g.doSearch(ctx, proxyReq, false)
	if err != nil {
		return nil, err
	}

	resp := &seqproxyapi.GetHistogramResponse{
		Hist:  makeProtoHistogram(sResp.qpr),
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
