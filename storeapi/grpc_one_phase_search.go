package storeapi

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func (g *GrpcV1) OnePhaseSearch(
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
	ctx, span := tracing.StartSpan(stream.Context(), "store-server/OnePhaseSearch")
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("request", req.Query))
		span.AddAttributes(trace.Int64Attribute("from", req.From))
		span.AddAttributes(trace.Int64Attribute("to", req.From))
		span.AddAttributes(trace.Int64Attribute("size", req.Size))
		span.AddAttributes(trace.Int64Attribute("offset", req.Offset))
		span.AddAttributes(trace.StringAttribute("offset_id", req.OffsetId))
		span.AddAttributes(trace.BoolAttribute("explain", req.Explain))
		span.AddAttributes(trace.BoolAttribute("with_total", req.WithTotal))
	}

	err := g.doOnePhaseSearch(ctx, req, stream)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		logger.Error("one phase search error", zap.Error(err))
	}
	return err
}

func (g *GrpcV1) doOnePhaseSearch(
	ctx context.Context,
	req *storeapi.OnePhaseSearchRequest,
	stream storeapi.StoreApi_OnePhaseSearchServer,
) error {
	tr := querytracer.New(req.Explain, "store/Search")
	data, err := g.doSearch(ctx, &storeapi.SearchRequest{
		Query:     req.Query,
		From:      req.From,
		To:        req.To,
		Size:      req.Size,
		Offset:    req.Offset,
		Explain:   req.Explain,
		WithTotal: req.WithTotal,
		Order:     req.Order,
		OffsetId:  req.OffsetId,
	}, tr)
	if err != nil {
		return fmt.Errorf("search error: %w", err)
	}

	tr.Done()
	if req.Explain && data != nil {
		data.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	}

	err = stream.Send(&storeapi.OnePhaseSearchResponse{
		ResponseType: &storeapi.OnePhaseSearchResponse_Header{
			Header: &storeapi.Header{
				Total:   data.Total,
				Errors:  data.Errors,
				Code:    data.Code,
				Explain: data.Explain,
			},
		},
	})
	if err != nil {
		if util.IsCancelled(ctx) {
			logger.Info("one phase search request is canceled")
			return nil
		}
		return fmt.Errorf("error sending fetched docs: %w", err)
	}

	ids := make(seq.IDSources, 0, len(data.IdSources))
	for _, id := range data.IdSources {
		ids = append(ids, seq.IDSource{
			ID:   seq.ID{MID: seq.MID(id.Id.Mid), RID: seq.RID(id.Id.Rid)},
			Hint: id.Hint,
		})
	}

	send := func(block []byte) error {
		return stream.Send(&storeapi.OnePhaseSearchResponse{
			ResponseType: &storeapi.OnePhaseSearchResponse_Document{
				Document: &storeapi.Document{
					Data: &storeapi.BinaryData{Data: block},
				},
			},
		})
	}
	err = g.doFetch(ctx, ids, req.FieldsFilter, req.Explain, send)
	if err != nil {
		return fmt.Errorf("fetch error: %w", err)
	}

	return nil
}
