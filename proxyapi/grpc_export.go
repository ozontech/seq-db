package proxyapi

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
)

type metricStream struct {
	seqproxyapi.SeqProxyApi_ExportServer
	size int
}

func (s *metricStream) Send(resp *seqproxyapi.ExportResponse) error {
	s.size += len(resp.GetDoc().GetData()) + len(resp.GetDoc().GetId())
	return s.SeqProxyApi_ExportServer.Send(resp)
}

func (g *grpcV1) Export(req *seqproxyapi.ExportRequest, stream seqproxyapi.SeqProxyApi_ExportServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), g.config.ExportTimeout)
	defer cancel()

	if conf.MaxRequestedDocuments > 0 && req.Size > int64(conf.MaxRequestedDocuments) {
		return status.Errorf(codes.InvalidArgument, "too many documents are requested: count=%d, max=%d",
			req.Size, conf.MaxRequestedDocuments)
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
	sResp, err := g.doSearch(ctx, proxyReq, true)
	if err != nil {
		return err
	}

	wrapped := metricStream{SeqProxyApi_ExportServer: stream}
	defer func() {
		metric.ExportSize.WithLabelValues(protocol).Observe(float64(wrapped.size))
	}()

	for doc, err := sResp.docsStream.Next(); err == nil; doc, err = sResp.docsStream.Next() {
		eResp := &seqproxyapi.ExportResponse{
			Doc: &seqproxyapi.Document{
				Id:   doc.ID.String(),
				Data: doc.Data,
			},
		}
		if err = wrapped.Send(eResp); err != nil {
			return status.Errorf(codes.Internal, "failed to send data: %v", err)
		}
	}

	return nil
}
