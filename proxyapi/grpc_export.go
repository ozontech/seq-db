package proxyapi

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
)

type metricStream struct {
	seqproxyapi.SeqProxyApi_ExportServer
	size int
}

func (s *metricStream) Send(resp *seqproxyapi.ExportResponse) error {
	s.size += len(resp.GetDoc().GetData()) + len(resp.GetDoc().GetId())
	return s.SeqProxyApi_ExportServer.Send(resp)
}

func (g *grpcV1) Export(req *seqproxyapi.ExportRequest, stream seqproxyapi.SeqProxyApi_ExportServer) (retErr error) {
	ctx, cancel := context.WithTimeout(stream.Context(), g.config.ExportTimeout)
	defer cancel()

	if config.MaxRequestedDocuments > 0 && req.Size > int64(config.MaxRequestedDocuments) {
		return status.Errorf(codes.InvalidArgument, "too many documents are requested: count=%d, max=%d",
			req.Size, config.MaxRequestedDocuments)
	}

	const protocol = "grpc"
	defer func(start time.Time) {
		metric.ExportDuration.WithLabelValues(protocol).Observe(float64(time.Since(start).Milliseconds()))
	}(time.Now())

	metric.CurrentExportersCount.WithLabelValues(protocol).Inc()
	defer metric.CurrentExportersCount.WithLabelValues(protocol).Dec()

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query:     req.Query,
		Size:      req.Size,
		Offset:    req.Offset,
		WithTotal: false,
	}
	sResp, obs, err := g.doSearch(ctx, proxyReq, true, true, nil)
	defer func() { obs.finish("Export", retErr) }()
	if err != nil {
		return err
	}
	if sResp.err != nil && sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE && shouldFailPartialResponse(ctx) {
		return status.Error(codes.Internal, "partial response: not all shards returned results")
	}
	if sResp.err != nil && !shouldHaveResponse(sResp.err.Code) {
		return errors.New(sResp.err.Message)
	}

	wrapped := metricStream{SeqProxyApi_ExportServer: stream}
	defer func() {
		metric.ExportSize.WithLabelValues(protocol).Observe(float64(wrapped.size))
	}()

	for doc, err := range search.DocsIteratorSeq(sResp.docsStream) {
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
