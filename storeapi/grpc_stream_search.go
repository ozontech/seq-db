package storeapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/exec"
	"github.com/ozontech/seq-db/query/plan"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

// streamSearchBatchSize limits the number of records sent in a single
// StreamSearchResponse data message.
const streamSearchBatchSize = 100

type controlOutcome int

const (
	outcomeNone     controlOutcome = iota // data exhausted, no control received yet
	outcomeFinalize                       // client requested a graceful finalization
	outcomeCancel                         // client canceled or disconnected
)

func (g *GrpcV1) StreamSearch(stream storeapi.StoreApi_StreamSearchServer) error {
	ctx, span := tracing.StartSpan(stream.Context(), "store-server/StreamSearch")
	defer span.End()

	// The first message must carry the search query.
	req, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	q := req.GetQuery()
	if q == nil {
		return status.Error(codes.InvalidArgument, "first message must be a search query")
	}

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("request", q.Query))
		span.AddAttributes(trace.StringAttribute("from", q.From.AsTime().Format(time.RFC3339Nano)))
		span.AddAttributes(trace.StringAttribute("to", q.To.AsTime().Format(time.RFC3339Nano)))
		span.AddAttributes(trace.StringAttribute("offset_id", q.OffsetId))
		span.AddAttributes(trace.BoolAttribute("explain", q.Explain))
		span.AddAttributes(trace.BoolAttribute("with_total", q.WithTotal))
	}

	err = g.doStreamSearch(ctx, q, stream)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		logger.Error("stream search error", zap.Error(err))
	}
	return err
}

func (g *GrpcV1) doStreamSearch(
	ctx context.Context,
	req *storeapi.StreamSearchQuery,
	stream storeapi.StoreApi_StreamSearchServer,
) error {
	metric.SearchInFlightQueriesTotal.Inc()
	defer metric.SearchInFlightQueriesTotal.Dec()

	inflightRequests := g.searchData.inflight.Inc()
	defer g.searchData.inflight.Dec()

	if inflightRequests > int64(g.config.Search.RequestsLimit) {
		metric.RejectedRequests.WithLabelValues("search", "limit_exceeding").Inc()
		return fmt.Errorf("too many search requests: %d > %d", inflightRequests, g.config.Search.RequestsLimit)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	tr := querytracer.New(req.Explain, "store/StreamSearchDocs")

	var errCode error
	// in store mode hot we return error in case request wants data, that we've already rotated
	if g.config.StoreMode == StoreModeHot {
		if g.fracManager.Flags().IsCapacityExceeded() && g.earlierThanOldestFrac(uint64(seq.TimeToMID(req.From.AsTime()))) {
			metric.RejectedRequests.WithLabelValues("search", "old_data").Inc()
			errCode = consts.ErrIngestorQueryWantsOldData
		}
	}

	// exit if had errors
	if errCode != nil {
		return sendSummary(stream, &query.Summary{Err: errCode}, tr, req.Explain)
	}

	parseQueryTr := tr.NewChild("parse query")
	seqql, err := parser.ParseSeqQL(req.Query, g.mappingProvider.GetMapping())
	if err != nil {
		parseQueryTr.Done()
		return fmt.Errorf("parse query error: %w", err)
	}
	parseQueryTr.Done()

	buildProducerTr := tr.NewChild("build producer")
	producer, schema, err := g.buildProducer(ctx, req, tr, seqql)
	if err != nil {
		buildProducerTr.Done()
		return fmt.Errorf("can't build record producer: %w", err)
	}
	buildProducerTr.Done()

	err = stream.Send(&storeapi.StreamSearchResponse{
		ResponseType: &storeapi.StreamSearchResponse_Header{
			Header: &storeapi.ResponseHeader{
				Typing: schemaToTyping(schema),
			},
		},
	})
	if err != nil {
		_ = producer.Finalize()
		if util.IsCancelled(ctx) {
			logger.Info("stream search request is canceled")
			return nil
		}
		return fmt.Errorf("error sending header: %w", err)
	}

	// Read control messages concurrently with sending data.
	controlCh := make(chan *storeapi.StreamControl)
	recvErrCh := make(chan error, 1)
	go func() {
		defer close(controlCh)
		defer close(recvErrCh)
		for {
			msg, err := stream.Recv()
			if err != nil {
				// io.EOF or any read error means the client is done. Signal it and stop reading.
				select {
				case recvErrCh <- err:
				case <-ctx.Done():
				}
				return
			}
			if c := msg.GetControl(); c != nil {
				select {
				case controlCh <- c:
				case <-ctx.Done():
					return
				}
			}
			// Any other message type on the input stream is ignored.
		}
	}()

	outcome, err := g.streamSearchRecords(ctx, stream, producer, controlCh, recvErrCh)
	if err != nil {
		_ = producer.Finalize()
		return fmt.Errorf("store can't stream records: %w", err)
	}

	// CANCEL: terminate immediately, no summary.
	if outcome == outcomeCancel {
		_ = producer.Finalize()
		return nil
	}

	metric.SearchDurationSeconds.Observe(time.Since(start).Seconds())

	summary := producer.Finalize()
	if summary == nil {
		summary = &query.Summary{}
	}
	return sendSummary(stream, summary, tr, req.Explain)
}

func (g *GrpcV1) streamSearchRecords(
	ctx context.Context,
	stream storeapi.StoreApi_StreamSearchServer,
	producer query.RecordProducer,
	controlCh <-chan *storeapi.StreamControl,
	recvErrCh <-chan error,
) (controlOutcome, error) {
	var batch []*storeapi.Record
	for curRecord := producer.Next(); curRecord != nil; curRecord = producer.Next() {
		rawData := make([][]byte, len(curRecord.Vals))
		for i, d := range curRecord.Vals {
			rawData[i] = d.RawData()
		}
		batch = append(batch, &storeapi.Record{RawData: rawData})

		if len(batch) >= streamSearchBatchSize {
			if err := sendRecords(stream, batch); err != nil {
				if util.IsCancelled(ctx) {
					logger.Info("stream search request is canceled")
					return outcomeCancel, nil
				}
				return outcomeNone, fmt.Errorf("error sending fetched docs: %w", err)
			}
			batch = batch[:0]
			if curOutcome, stop := checkControl(controlCh, recvErrCh, ctx); stop {
				return curOutcome, nil
			}
		}
	}
	if len(batch) > 0 {
		if err := sendRecords(stream, batch); err != nil {
			if util.IsCancelled(ctx) {
				logger.Info("stream search request is canceled")
				return outcomeCancel, nil
			}
			return outcomeNone, fmt.Errorf("error sending fetched docs: %w", err)
		}
	}
	return outcomeFinalize, nil
}

// checkControl peeks at the control/recv channels without blocking. It returns
// ok=true when the caller should stop streaming (a control action arrived or
// the client disconnected).
func checkControl(
	controlCh <-chan *storeapi.StreamControl,
	recvErrCh <-chan error,
	ctx context.Context,
) (controlOutcome, bool) {
	select {
	case c, ok := <-controlCh:
		if !ok {
			return outcomeNone, false
		}
		if c.GetAction() == storeapi.ControlAction_CANCEL {
			return outcomeCancel, true
		}
		return outcomeFinalize, true
	case err, ok := <-recvErrCh:
		if !ok {
			return outcomeNone, false
		}
		if errors.Is(err, io.EOF) {
			return outcomeNone, false
		}
		return outcomeCancel, true
	case <-ctx.Done():
		return outcomeCancel, true
	default:
		return outcomeNone, false
	}
}

func sendRecords(stream storeapi.StoreApi_StreamSearchServer, records []*storeapi.Record) error {
	resp := &storeapi.StreamSearchResponse{
		ResponseType: &storeapi.StreamSearchResponse_Data{
			Data: &storeapi.ResponseData{
				Batch: &storeapi.RecordsBatch{Records: records},
			},
		},
	}
	if err := stream.Send(resp); err != nil {
		return fmt.Errorf("error sending data: %w", err)
	}
	return nil
}

func sendSummary(
	stream storeapi.StoreApi_StreamSearchServer,
	summary *query.Summary,
	tr *querytracer.Tracer,
	explain bool,
) error {
	respSummary := &storeapi.ResponseSummary{
		Total: summary.Total,
		Error: &storeapi.Error{Code: storeapi.SearchErrorCode_NO_ERROR},
	}

	if summary.Err != nil {
		errCode, _ := parseStoreError(summary.Err)
		respSummary.Error = &storeapi.Error{
			Code:    errCode,
			Message: summary.Err.Error(),
		}
	}

	if explain {
		tr.Done()
		respSummary.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	}

	err := stream.Send(&storeapi.StreamSearchResponse{
		ResponseType: &storeapi.StreamSearchResponse_Summary{Summary: respSummary},
	})
	if err != nil {
		return fmt.Errorf("error sending summary: %w", err)
	}
	return nil
}

// buildProducer translates the logical plan into the physical one. Returns the plan's output schema.
func (g *GrpcV1) buildProducer(
	ctx context.Context,
	req *storeapi.StreamSearchQuery,
	tr *querytracer.Tracer,
	seqql parser.SeqQLQuery,
) (query.RecordProducer, *query.Schema, error) {
	p, err := plan.Build(plan.BuildParams{
		SeqQL:     &seqql,
		Input:     query.DocsSchema,
		DocField:  query.DocsDataCol,
		From:      seq.MillisToMID(uint64(seq.TimeToMID(req.From.AsTime()))),
		To:        seq.MillisToMID(uint64(seq.TimeToMID(req.To.AsTime()))),
		OffsetID:  req.OffsetId,
		WithTotal: req.WithTotal,
	})
	if err != nil {
		return nil, nil, err
	}

	// The data source is limitless and walks the matched set via cursor pagination;
	// the real request limit is applied by a Limiter executor. The limit op
	// also caps the per-batch scan size.
	searchParams := processor.SearchParams{
		AST:       p.Scan.AST,
		From:      p.Scan.From,
		To:        p.Scan.To,
		WithTotal: p.Scan.WithTotal,
		Order:     p.Scan.Order,
		OffsetId:  p.Scan.OffsetID,
		AggQ:      p.Scan.AggQ,
	}
	for _, op := range p.Ops {
		if op, ok := op.(*plan.LimitOp); ok {
			searchParams.Limit = op.Limit
		}
	}

	var producer query.RecordProducer = exec.NewSearcherDataSource(
		ctx,
		tr,
		searchParams,
		g.fracManager,
		g.searchData.searcher,
		g.fetchData.docFetcher,
	)

	if p.IsAgg() {
		return producer, p.Schema, nil
	}

	// ops wrap the source, the schema flows through them so an op that changes the record
	// shape extends it here and downstream ops resolve their columns from the extension
	schema := p.Schema
	for _, op := range p.Ops {
		producer, schema = op.Apply(producer, schema)
	}

	return producer, schema, nil
}

func schemaToTyping(s *query.Schema) []*storeapi.Typing {
	cols := s.Cols()
	out := make([]*storeapi.Typing, 0, len(cols))
	for _, c := range cols {
		out = append(out, &storeapi.Typing{
			Title: c.Name,
			Type:  storeapi.MustProtoDataType(c.Type),
		})
	}
	return out
}
