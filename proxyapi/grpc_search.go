package proxyapi

import (
	"context"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
)

func (g *grpcV1) Search(
	ctx context.Context, req *seqproxyapi.SearchRequest,
) (*seqproxyapi.SearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.config.SearchTimeout)
	defer cancel()

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query:     req.Query,
		Size:      req.Size,
		Offset:    req.Offset,
		WithTotal: req.WithTotal,
		Order:     req.Order,
	}
	sResp, err := g.doSearch(ctx, proxyReq, true)
	if err != nil {
		return nil, err
	}

	resp := &seqproxyapi.SearchResponse{
		Docs:  makeProtoDocs(sResp.qpr, sResp.docsStream),
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
