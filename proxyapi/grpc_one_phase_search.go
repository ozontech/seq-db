package proxyapi

import (
	"context"
	"errors"
	"time"

	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) OnePhaseSearch(ctx context.Context, req *seqproxyapi.SearchRequest) (*seqproxyapi.SearchResponse, error) {
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
	sResp, err := g.doOnePhaseSearch(ctx, proxyReq, true)
	if err != nil {
		return nil, err
	}
	if sResp.err != nil && !shouldHaveResponse(sResp.err.Code) {
		return &seqproxyapi.SearchResponse{Error: sResp.err}, nil
	}

	resp := &seqproxyapi.SearchResponse{
		Docs:  makeProtoDocsKek(sResp.docsStream),
		Total: int64(sResp.qpr.Total),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}
	if sResp.err != nil {
		resp.Error = sResp.err
	}

	return resp, nil
}

func makeProtoDocsKek(docs search.DocsIterator) []*seqproxyapi.Document {
	// TODO: paginate here (???)
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
) (*proxySearchResponse, error) {
	metric.SearchOverall.Add(1)

	span := trace.FromContext(ctx)
	defer span.End()

	if req.Query == nil {
		return nil, status.Error(codes.InvalidArgument, "search query must be provided")
	}
	if req.Query.From == nil || req.Query.To == nil {
		return nil, status.Error(codes.InvalidArgument, `search query "from" and "to" fields must be provided`)
	}
	if req.Offset != 0 && req.OffsetId != "" {
		return nil, status.Error(codes.InvalidArgument, `only one of "offset" and "offset_id" must be provided`)
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
		return nil, status.Error(codes.ResourceExhausted, consts.ErrRequestWasRateLimited.Error())
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
	qpr, docsStream, err := g.searchIngestor.OnePhaseSearch(ctx, proxyReq, tr)
	psr := &proxySearchResponse{
		qpr:        qpr,
		docsStream: docsStream,
	}

	if e, ok := parseProxyError(err); ok {
		psr.err = e
		return psr, nil
	}

	if errors.Is(err, consts.ErrInvalidArgument) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if st, ok := status.FromError(err); ok {
		// could not parse a query
		if st.Code() == codes.InvalidArgument {
			return nil, err
		}
	}

	if errors.Is(err, consts.ErrPartialResponse) {
		metric.SearchPartial.Inc()
		psr.err = &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE,
			Message: err.Error(),
		}
		return psr, nil
	}
	if err = processSearchErrors(qpr, err); err != nil {
		metric.SearchErrors.Inc()
		return nil, err
	}

	g.tryMirrorRequest(req)

	return psr, nil
}
