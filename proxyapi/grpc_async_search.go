package proxyapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/asyncsearcher"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

func (g *grpcV1) StartAsyncSearch(
	ctx context.Context,
	r *seqproxyapi.StartAsyncSearchRequest,
) (*seqproxyapi.StartAsyncSearchResponse, error) {
	if g.config.AsyncSearchMaxDocumentsPerRequest > 0 && r.Size > g.config.AsyncSearchMaxDocumentsPerRequest {
		return nil, status.Errorf(codes.InvalidArgument, "too many documents are requested: count=%d, max=%d",
			r.Size, g.config.AsyncSearchMaxDocumentsPerRequest)
	}

	// reject "empty" request: no docs, no aggs, no hist
	if r.WithDocs == false && len(r.Aggs) == 0 && r.Hist == nil {
		return nil, status.Error(codes.InvalidArgument, "can't serve empty request: fill aggs, hist or withDocs")
	}

	aggs, err := convertAggsQuery(r.Aggs)
	if err != nil {
		return nil, err
	}

	var histInterval time.Duration
	if r.Hist != nil {
		histInterval, err = util.ParseDuration(r.Hist.Interval)
		if err != nil {
			return nil, fmt.Errorf("error parsing hist interval: %w", err)
		}
	}

	resp, err := g.searchIngestor.StartAsyncSearch(ctx, search.AsyncRequest{
		Retention:         r.Retention.AsDuration(),
		Query:             r.GetQuery().GetQuery(),
		From:              r.GetQuery().GetFrom().AsTime(),
		To:                r.GetQuery().GetTo().AsTime(),
		Aggregations:      aggs,
		HistogramInterval: seq.MID(histInterval.Nanoseconds()),
		WithDocs:          r.WithDocs,
		Size:              r.Size,
	})
	if err != nil {
		return nil, err
	}
	return &seqproxyapi.StartAsyncSearchResponse{
		SearchId: resp.ID,
	}, nil
}

func (g *grpcV1) FetchAsyncSearchResult(
	ctx context.Context,
	r *seqproxyapi.FetchAsyncSearchResultRequest,
) (*seqproxyapi.FetchAsyncSearchResultResponse, error) {
	resp, stream, fetchErr := g.searchIngestor.FetchAsyncSearchResult(ctx, search.FetchAsyncSearchResultRequest{
		ID:     r.SearchId,
		Size:   int(r.Size),
		Offset: int(r.Offset),
		Order:  r.Order.MustDocsOrder(),
	})
	if fetchErr != nil && !errors.Is(fetchErr, consts.ErrPartialResponse) {
		return nil, fetchErr
	}

	var canceledAt *timestamppb.Timestamp
	if !resp.CanceledAt.IsZero() {
		canceledAt = timestamppb.New(resp.CanceledAt)
	}

	docs := makeProtoDocs(&resp.QPR, stream)

	searchReq := &seqproxyapi.StartAsyncSearchRequest{
		Retention: durationpb.New(resp.Request.Retention),
		Query: &seqproxyapi.SearchQuery{
			Query: resp.Request.Query,
			From:  timestamppb.New(resp.Request.From),
			To:    timestamppb.New(resp.Request.To),
		},
		Aggs:     makeProtoRequestAggregations(resp.Request.Aggregations),
		WithDocs: resp.Request.WithDocs,
		Size:     resp.Request.Size,
	}
	if resp.Request.HistogramInterval > 0 {
		searchReq.Hist = &seqproxyapi.HistQuery{
			Interval: seq.MIDToDuration(resp.Request.HistogramInterval).String(),
		}
	}

	var responseErr *seqproxyapi.Error
	if len(resp.QPR.Errors) > 0 {
		errs := make([]error, 0, len(resp.QPR.Errors))
		for _, e := range resp.QPR.Errors {
			errs = append(errs, errors.New(e.ErrStr))
		}
		responseErr = &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_UNSPECIFIED,
			Message: util.DeduplicateErrors(errs).Error(),
		}
	}

	res := &seqproxyapi.FetchAsyncSearchResultResponse{
		Status:  seqproxyapi.MustProtoAsyncSearchStatus(resp.Status),
		Request: searchReq,
		Response: &seqproxyapi.ComplexSearchResponse{
			Total:   int64(resp.QPR.Total),
			Docs:    docs,
			Aggs:    makeProtoAggregation(resp.AggResult),
			Hist:    makeProtoHistogram(&resp.QPR),
			Error:   responseErr,
			Explain: nil,
		},
		StartedAt:  timestamppb.New(resp.StartedAt),
		ExpiresAt:  timestamppb.New(resp.ExpiresAt),
		CanceledAt: canceledAt,
		Progress:   resp.Progress,
		DiskUsage:  resp.DiskUsage,
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}

	if fetchErr != nil && errors.Is(fetchErr, consts.ErrPartialResponse) {
		res.Error = &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE,
			Message: fetchErr.Error(),
		}
	}

	return res, nil
}

func (g *grpcV1) GetAsyncSearchesList(
	ctx context.Context,
	r *seqproxyapi.GetAsyncSearchesListRequest,
) (*seqproxyapi.GetAsyncSearchesListResponse, error) {
	var searchStatus *asyncsearcher.AsyncSearchStatus
	if r.Status != nil {
		s := r.Status.MustAsyncSearchStatus()
		searchStatus = &s
	}

	req := search.GetAsyncSearchesListRequest{
		Status: searchStatus,
		Size:   int(r.Size),
		Offset: int(r.Offset),
		IDs:    r.Ids,
	}

	searches, listErr := g.searchIngestor.GetAsyncSearchesList(ctx, req)
	if listErr != nil && !errors.Is(listErr, consts.ErrPartialResponse) {
		return nil, listErr
	}

	res := &seqproxyapi.GetAsyncSearchesListResponse{
		Searches: makeProtoAsyncSearchesList(searches),
		Error: &seqproxyapi.Error{
			Code: seqproxyapi.ErrorCode_ERROR_CODE_NO,
		},
	}

	if listErr != nil && errors.Is(listErr, consts.ErrPartialResponse) {
		res.Error = &seqproxyapi.Error{
			Code:    seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE,
			Message: listErr.Error(),
		}
	}

	return res, nil
}

type asyncExportStream struct {
	ExportAsyncSearchServer
	size int
}

func (s *asyncExportStream) Send(resp *seqproxyapi.ExportResponse) error {
	s.size += len(resp.GetDoc().GetData()) + len(resp.GetDoc().GetId())
	return s.ExportAsyncSearchServer.Send(resp)
}

func (g *grpcV1) ExportAsyncSearch(req *seqproxyapi.ExportAsyncSearchRequest, stream seqproxyapi.SeqProxyApi_ExportAsyncSearchServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), g.config.ExportTimeout)
	defer cancel()

	if g.config.AsyncSearchMaxDocumentsPerRequest > 0 && req.Size > g.config.AsyncSearchMaxDocumentsPerRequest {
		return status.Errorf(codes.InvalidArgument, "too many documents are requested: count=%d, max=%d",
			req.Size, g.config.AsyncSearchMaxDocumentsPerRequest)
	}

	const protocol = "grpc"
	defer func(start time.Time) {
		asyncsearcher.ExportDuration.WithLabelValues(protocol).Observe(float64(time.Since(start).Milliseconds()))
	}(time.Now())

	asyncsearcher.CurrentExportersCount.WithLabelValues(protocol).Inc()
	defer asyncsearcher.CurrentExportersCount.WithLabelValues(protocol).Dec()

	_, docsStream, err := g.searchIngestor.FetchAsyncSearchResult(ctx, search.FetchAsyncSearchResultRequest{
		ID:     req.SearchId,
		Size:   int(req.Size),
		Offset: int(req.Offset),
	})
	if err != nil {
		return err
	}

	wrapped := asyncExportStream{ExportAsyncSearchServer: stream}
	defer func() {
		asyncsearcher.ExportSize.WithLabelValues(protocol).Observe(float64(wrapped.size))
	}()

	for doc, err := range search.DocsIteratorSeq(docsStream) {
		if err != nil {
			return status.Errorf(codes.Internal, "docs reading error: %v", err)
		}
		eResp := &seqproxyapi.ExportResponse{
			Doc: &seqproxyapi.Document{
				Id:   doc.ID.String(),
				Data: doc.Data,
				Time: timestamppb.New(doc.ID.MID.Time()),
			},
		}
		if err = wrapped.Send(eResp); err != nil {
			return status.Errorf(codes.Internal, "failed to send data: %v", err)
		}
	}

	return nil
}

func (g *grpcV1) CancelAsyncSearch(
	ctx context.Context,
	r *seqproxyapi.CancelAsyncSearchRequest,
) (*seqproxyapi.CancelAsyncSearchResponse, error) {
	if err := g.searchIngestor.CancelAsyncSearch(ctx, r.SearchId); err != nil {
		return nil, fmt.Errorf("cancelling search: %s", err)
	}
	return &seqproxyapi.CancelAsyncSearchResponse{}, nil
}

func (g *grpcV1) DeleteAsyncSearch(
	ctx context.Context,
	r *seqproxyapi.DeleteAsyncSearchRequest,
) (*seqproxyapi.DeleteAsyncSearchResponse, error) {
	if err := g.searchIngestor.DeleteAsyncSearch(ctx, r.SearchId); err != nil {
		return nil, fmt.Errorf("deleting search: %s", err)
	}
	return &seqproxyapi.DeleteAsyncSearchResponse{}, nil
}

func makeProtoRequestAggregations(sourceAggs []search.AggQuery) []*seqproxyapi.AggQuery {
	aggs := make([]*seqproxyapi.AggQuery, 0, len(sourceAggs))
	for _, a := range sourceAggs {
		agg := &seqproxyapi.AggQuery{
			Field:     a.Field,
			GroupBy:   a.GroupBy,
			Func:      seqproxyapi.AggFunc(a.Func),
			Quantiles: a.Quantiles,
		}

		if a.Interval != 0 {
			interval := seq.MIDToDuration(a.Interval).String()
			agg.Interval = &interval
		}

		// Support legacy format in which field means groupBy.
		if agg.Func == seq.AggFuncCount && agg.GroupBy != "" {
			agg.Field = agg.GroupBy
			agg.GroupBy = ""
		}

		aggs = append(aggs, agg)
	}
	return aggs
}

func makeProtoAsyncSearchesList(in []*search.AsyncSearchesListItem) []*seqproxyapi.AsyncSearchesListItem {
	searches := make([]*seqproxyapi.AsyncSearchesListItem, 0, len(in))
	for _, s := range in {
		var canceledAt *timestamppb.Timestamp
		if !s.CanceledAt.IsZero() {
			canceledAt = timestamppb.New(s.CanceledAt)
		}

		searchReq := &seqproxyapi.StartAsyncSearchRequest{
			Retention: durationpb.New(s.Request.Retention),
			Query: &seqproxyapi.SearchQuery{
				Query: s.Request.Query,
				From:  timestamppb.New(s.Request.From),
				To:    timestamppb.New(s.Request.To),
			},
			Aggs:     makeProtoRequestAggregations(s.Request.Aggregations),
			WithDocs: s.Request.WithDocs,
			Size:     s.Request.Size,
		}
		if s.Request.HistogramInterval > 0 {
			searchReq.Hist = &seqproxyapi.HistQuery{
				Interval: seq.MIDToDuration(s.Request.HistogramInterval).String(),
			}
		}

		var reqErr *string
		if s.Error != nil {
			errStr := s.Error.Error()
			reqErr = &errStr
		}

		searches = append(searches, &seqproxyapi.AsyncSearchesListItem{
			SearchId:   s.ID,
			Status:     seqproxyapi.MustProtoAsyncSearchStatus(s.Status),
			Request:    searchReq,
			StartedAt:  timestamppb.New(s.StartedAt),
			ExpiresAt:  timestamppb.New(s.ExpiresAt),
			CanceledAt: canceledAt,
			Progress:   s.Progress,
			DiskUsage:  s.DiskUsage,
			Error:      reqErr,
		})
	}

	return searches
}
