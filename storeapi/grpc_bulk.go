package storeapi

import (
	"context"
	"fmt"
	"time"

	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/tracing"
)

func (g *GrpcV1) Bulk(ctx context.Context, req *storeapi.BulkRequest) (*emptypb.Empty, error) {
	ctx, span := tracing.StartSpan(ctx, "store-server/Bulk")
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.Int64Attribute("count", req.Count))
	}

	err := g.doBulk(ctx, req)
	if err != nil {
		logger.Error("bulk error", zap.Error(err))
	}
	return &g.blank, err
}

func (g *GrpcV1) doBulk(ctx context.Context, req *storeapi.BulkRequest) error {
	if req.Count == 0 {
		return fmt.Errorf("wrong protocol, count=0: %v", req)
	}

	metric.BulkInFlightQueriesTotal.Inc()
	defer metric.BulkInFlightQueriesTotal.Dec()

	inflightRequests := g.bulkData.inflight.Inc()
	defer g.bulkData.inflight.Dec()

	if inflightRequests > int64(g.config.Bulk.RequestsLimit) {
		metric.RejectedRequests.WithLabelValues("bulk", "limit_exceeding").Inc()
		return fmt.Errorf("too many bulk requests: %d > %d", inflightRequests, g.config.Bulk.RequestsLimit)
	}

	g.bulkData.appendQueue.Inc()
	g.bulkData.writeQueue.Inc()
	start := time.Now()

	err := g.fracManager.Append(ctx, req.Docs, req.Metas, g.bulkData.writeQueue)

	g.bulkData.appendQueue.Dec()

	if err != nil {
		return err
	}

	overallDuration := time.Since(start)
	metric.BulkDurationSeconds.Observe(float64(overallDuration) / float64(time.Second))
	metric.BulkDocsTotal.Observe(float64(req.Count))
	metric.BulkDocBytesTotal.Observe(float64(len(req.Docs)))
	metric.BulkMetaBytesTotal.Observe(float64(len(req.Metas)))

	g.bulkData.took.Add(uint64(overallDuration.Nanoseconds()))
	g.bulkData.batches.Inc()

	if g.config.Bulk.LogThreshold != 0 && overallDuration >= g.config.Bulk.LogThreshold {
		logger.Warn("slow bulk",
			zap.Int64("took_ms", overallDuration.Milliseconds()),
			zap.Int64("count", req.Count),
			zap.Int64("inflight_requests", inflightRequests),
		)
	}
	return nil
}
