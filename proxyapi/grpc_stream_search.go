package proxyapi

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// streamSearchBatchSize limits the number of records sent in a single
// StreamSearchResponse data message.
const streamSearchBatchSize = 100

// controlOutcome describes how the data streaming phase ended.
type controlOutcome int

const (
	outcomeNone     controlOutcome = iota // data exhausted, no control received yet
	outcomeFinalize                       // client requested a graceful finalization
	outcomeCancel                         // client canceled or disconnected
)

func (g *grpcV1) StreamSearch(stream seqproxyapi.SeqProxyApi_StreamSearchServer) (retErr error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

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

	metric.SearchOverall.Add(1)

	searchReq, err := buildSearchReq(q)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("error parsing query: %s", err.Error()))
	}

	if searchReq.Agg != nil && (searchReq.Agg.Func == seq.AggFuncQuantile || searchReq.Agg.Func == seq.AggFuncUniqueCount) {
		// TODO: support all agg funcs
		return status.Error(codes.InvalidArgument, `unsupported aggregate function`)
	}

	tr := querytracer.New(q.Explain, "proxy/StreamSearch")

	var partialErr error
	storesStream, broadcaster, err := g.searchIngestor.StreamSearch(ctx, searchReq, tr)
	if err != nil {
		// The stores were not opened or failed to open, cancel any that may have started before propagating the error.
		if broadcaster != nil {
			broadcaster.SendControl(storeapi.ControlAction_CANCEL)
		}
		if errors.Is(err, consts.ErrPartialResponse) {
			if shouldFailPartialResponse(ctx) {
				return status.Error(codes.Internal, "partial response: not all shards returned results")
			}
			partialErr = err
			metric.SearchPartial.Inc()
		} else {
			return status.Error(codes.Internal, err.Error())
		}
	}

	// Read control messages from the client concurrently with sending data.
	controlCh := make(chan *seqproxyapi.StreamControl)
	recvErrCh := make(chan error, 1)
	go func() {
		defer close(controlCh)
		defer close(recvErrCh)
		for {
			msg, err := stream.Recv()
			if err != nil {
				// io.EOF or any read error means the client is done. Signal it and stop reading
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

	var typing []*seqproxyapi.Typing
	var toRecord func(*query.Record) *seqproxyapi.Record
	if searchReq.Agg != nil {
		typing = aggsTyping()
		toRecord = aggToRecord
	} else {
		typing = docsTyping()
		toRecord = docToRecord
	}
	outcome, err := g.streamSearchRecords(stream, storesStream, typing, toRecord, controlCh, recvErrCh, ctx)
	if err != nil {
		// Streaming failed: cancel the stores so they stop producing.
		broadcaster.SendControl(storeapi.ControlAction_CANCEL)
		return err
	}

	// CANCEL: terminate immediately, no summary.
	if outcome == outcomeCancel {
		broadcaster.SendControl(storeapi.ControlAction_CANCEL)
		return nil
	}

	// FINALIZE or data exhausted without an explicit control action: send the summary gathered from the store stream.
	broadcaster.SendControl(storeapi.ControlAction_FINALIZE)
	summary := storesStream.Finalize()
	if summary == nil {
		summary = &query.Summary{}
	}
	if partialErr != nil && summary.Err == nil {
		summary.Err = partialErr
	}
	return g.sendSummary(stream, summary, tr, q.Explain)
}

func (g *grpcV1) sendSummary(
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
	meta *query.Summary,
	tr *querytracer.Tracer,
	explain bool,
) error {
	summary := &seqproxyapi.ResponseSummary{
		Error: &seqproxyapi.Error{Code: seqproxyapi.ErrorCode_ERROR_CODE_NO},
	}

	if meta != nil {
		summary.Total = meta.Total
		if meta.Err != nil {
			summary.Error = &seqproxyapi.Error{
				Code:    mapProxyErrorCode(meta.Err),
				Message: meta.Err.Error(),
			}
		}
	}

	if explain {
		tr.Done()
		summary.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	}

	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		ResponseType: &seqproxyapi.StreamSearchResponse_Summary{Summary: summary},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send summary: %v", err)
	}
	return nil
}

func mapProxyErrorCode(err error) seqproxyapi.ErrorCode {
	switch {
	case errors.Is(err, consts.ErrPartialResponse):
		return seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE
	case errors.Is(err, consts.ErrTooManyFractionsHit):
		return seqproxyapi.ErrorCode_ERROR_CODE_TOO_MANY_FRACTIONS_HIT
	default:
		return seqproxyapi.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
}

// checkControl peeks at the control/recv channels without blocking. It returns
// ok=true when the caller should stop streaming (a control action arrived or
// the client disconnected).
func checkControl(
	controlCh <-chan *seqproxyapi.StreamControl,
	recvErrCh <-chan error,
	ctx context.Context,
) (controlOutcome, bool) {
	select {
	case c, ok := <-controlCh:
		if !ok {
			return outcomeNone, false
		}
		if c.GetAction() == seqproxyapi.ControlAction_CANCEL {
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

func (g *grpcV1) streamSearchRecords(
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
	storesStream query.RecordProducer,
	typing []*seqproxyapi.Typing,
	toRecord func(*query.Record) *seqproxyapi.Record,
	controlCh <-chan *seqproxyapi.StreamControl,
	recvErrCh <-chan error,
	ctx context.Context,
) (controlOutcome, error) {
	header := &seqproxyapi.ResponseHeader{Typing: typing}
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		ResponseType: &seqproxyapi.StreamSearchResponse_Header{Header: header},
	}); err != nil {
		return outcomeNone, status.Errorf(codes.Internal, "failed to send header: %v", err)
	}

	var batch []*seqproxyapi.Record
	for doc := storesStream.Next(); doc != nil; doc = storesStream.Next() {
		batch = append(batch, toRecord(doc))
		if len(batch) >= streamSearchBatchSize {
			if err := sendRecords(stream, batch); err != nil {
				return outcomeNone, err
			}
			batch = batch[:0]
			if curOutcome, stop := checkControl(controlCh, recvErrCh, ctx); stop {
				return curOutcome, nil
			}
		}
	}
	if len(batch) > 0 {
		if err := sendRecords(stream, batch); err != nil {
			return outcomeNone, err
		}
	}
	return outcomeFinalize, nil
}

func sendRecords(stream seqproxyapi.SeqProxyApi_StreamSearchServer, records []*seqproxyapi.Record) error {
	resp := &seqproxyapi.StreamSearchResponse{
		ResponseType: &seqproxyapi.StreamSearchResponse_Data{Data: &seqproxyapi.ResponseData{
			Batch: &seqproxyapi.RecordsBatch{Records: records},
		}},
	}
	if err := stream.Send(resp); err != nil {
		return status.Errorf(codes.Internal, "failed to send data: %v", err)
	}
	return nil
}

// hardcoded schema
func docsTyping() []*seqproxyapi.Typing {
	return []*seqproxyapi.Typing{
		{Title: "id", Type: seqproxyapi.DataType_SEQ_ID},
		{Title: "time", Type: seqproxyapi.DataType_UINT64},
		{Title: "data", Type: seqproxyapi.DataType_RAW_DOCUMENT},
	}
}

// hardcoded schema
func aggsTyping() []*seqproxyapi.Typing {
	return []*seqproxyapi.Typing{
		{Title: "key", Type: seqproxyapi.DataType_STRING},
		{Title: "value", Type: seqproxyapi.DataType_FLOAT64},
		{Title: "ts", Type: seqproxyapi.DataType_UINT64},
	}
}

// converts *query.Record to *seqproxyapi.Record according to hardcoded schemas from both store and proxy
func docToRecord(r *query.Record) *seqproxyapi.Record {
	id := r.Vals[0].Decoded().(seq.ID)

	return &seqproxyapi.Record{
		RawData: [][]byte{
			[]byte(id.String()),                    // id
			encoding.Uint64ToBytes(uint64(id.MID)), // time
			r.Vals[1].RawData(),                    // data
		},
	}
}

// converts *query.Record to *seqproxyapi.Record according to hardcoded schemas from both store and proxy
func aggToRecord(r *query.Record) *seqproxyapi.Record {
	return &seqproxyapi.Record{
		RawData: [][]byte{
			r.Vals[0].RawData(), // key
			r.Vals[1].RawData(), // value
			r.Vals[2].RawData(), // ts
		},
	}
}

func buildSearchReq(q *seqproxyapi.StreamSearchQuery) (*search.StreamSearchRequest, error) {
	seqql, err := parser.ParseSeqQL(q.Query, nil)
	if err != nil {
		return nil, err
	}

	streamSearchReq := &search.StreamSearchRequest{
		Query:     q.Query,
		From:      seq.TimeToMID(q.From.AsTime()),
		To:        seq.TimeToMID(q.To.AsTime()),
		Explain:   q.Explain,
		WithTotal: q.WithTotal,
		OffsetId:  q.OffsetId,
	}

	var (
		hasStatsPipe  bool
		hasOtherPipes bool
	)

	for _, pipe := range seqql.Pipes {
		switch p := pipe.(type) {
		case *parser.PipeLimit:
			streamSearchReq.Size = p.Limit
			hasOtherPipes = true
		case *parser.PipeOffset:
			streamSearchReq.Offset = p.Offset
			hasOtherPipes = true
		case *parser.PipeSort:
			order := seq.DocsOrderDesc
			if p.Order == "asc" {
				order = seq.DocsOrderAsc
			}
			streamSearchReq.Order = order
			hasOtherPipes = true
		case *parser.PipeFilter, *parser.PipeFields:
			hasOtherPipes = true
		case *parser.PipeStats:
			agg := p.Agg
			proxyReqAgg := &search.AggQuery{
				Field:     agg.Field,
				GroupBy:   agg.GroupBy,
				Func:      mustConvertStringToAggFunc(agg.Func),
				Quantiles: agg.Quantiles,
			}
			if agg.Interval != "" {
				interval, err := util.ParseDuration(agg.Interval)
				if err != nil {
					return nil, fmt.Errorf("failed to parse interval: %w", err)
				}
				proxyReqAgg.Interval = seq.DurationToMID(interval)
			}
			streamSearchReq.Agg = proxyReqAgg
			hasStatsPipe = true
		default:
			continue
		}
	}

	// for now we don't allow to combine stats with other pipes
	if hasStatsPipe && hasOtherPipes {
		return nil, errors.New("must be no other pipes if `stats` is present")
	}

	return streamSearchReq, nil
}

func mustConvertStringToAggFunc(funcName string) seq.AggFunc {
	switch funcName {
	case "count":
		return seq.AggFuncCount
	case "sum":
		return seq.AggFuncSum
	case "min":
		return seq.AggFuncMin
	case "max":
		return seq.AggFuncMax
	case "avg":
		return seq.AggFuncAvg
	case "quantile":
		return seq.AggFuncQuantile
	case "unique":
		return seq.AggFuncUnique
	case "unique_count":
		return seq.AggFuncUniqueCount
	default:
		panic(fmt.Errorf("unknown aggregation function: %s", funcName))
	}
}
