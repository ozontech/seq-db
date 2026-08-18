package proxyapi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

// streamSearchBatchSize limits the number of records sent in a single
// StreamSearchResponse data message.
const streamSearchBatchSize = 50

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
		return nil // Client closed the stream gracefully.
	}
	if err != nil {
		return err
	}
	q := req.GetQuery()
	if q == nil {
		return status.Error(codes.InvalidArgument, "first message must be a search query")
	}

	proxyReq, err := buildProxyReq(q)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("error parsing query: %s", err.Error()))
	}
	if proxyReq.Size <= 0 && len(proxyReq.Aggs) == 0 {
		return status.Error(codes.InvalidArgument, `one of "limit" or "stats" must be provided`)
	}
	if len(proxyReq.Aggs) > 1 {
		return status.Error(codes.InvalidArgument, `must be only one aggregation`)
	}

	tr := querytracer.New(q.Explain, "proxy/StreamSearch")
	sResp, obs, err := g.doSearch(ctx, proxyReq, true, false, tr)
	defer func() { obs.finish("StreamSearch", retErr) }()
	if err != nil {
		return err
	}
	if sResp.err != nil && sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE && shouldFailPartialResponse(ctx) {
		return status.Error(codes.Internal, "partial response: not all shards returned results")
	}
	if sResp.err != nil && !shouldHaveResponse(sResp.err.Code) {
		return errors.New(sResp.err.Message)
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
				// io.EOF or any read error means the client is done. Signal it
				// and stop reading.
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

	var outcome controlOutcome
	if len(proxyReq.Aggs) > 0 {
		outcome, err = g.streamSearchAggs(stream, proxyReq.Aggs, sResp, tr, controlCh, recvErrCh, ctx)
	} else {
		outcome, err = g.streamSearchDocs(stream, sResp, controlCh, recvErrCh, ctx)
	}
	if err != nil {
		return err
	}

	// CANCEL: terminate immediately, no summary.
	if outcome == outcomeCancel {
		return nil
	}

	// FINALIZE or data exhausted without an explicit control action: send the
	// summary.
	summary := &seqproxyapi.ResponseSummary{Total: sResp.qpr.Total}
	if sResp.err != nil {
		summary.Error = sResp.err
	} else {
		summary.Error = &seqproxyapi.Error{Code: seqproxyapi.ErrorCode_ERROR_CODE_NO}
	}
	tr.Done()
	summary.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		ResponseType: &seqproxyapi.StreamSearchResponse_Summary{Summary: summary},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send summary: %v", err)
	}

	return nil
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
	case c := <-controlCh:
		if c.GetAction() == seqproxyapi.ControlAction_CANCEL {
			return outcomeCancel, true
		}
		return outcomeFinalize, true
	case <-recvErrCh:
		return outcomeCancel, true
	case <-ctx.Done():
		return outcomeCancel, true
	default:
		return outcomeNone, false
	}
}

// streamSearchDocs streams matched documents as batches of records. Each record
// carries three columns: id (SEQ_ID), time (UINT64 nanoseconds) and data (RAW_DOCUMENT).
func (g *grpcV1) streamSearchDocs(
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
	sResp *proxySearchResponse,
	controlCh <-chan *seqproxyapi.StreamControl,
	recvErrCh <-chan error,
	ctx context.Context,
) (controlOutcome, error) {
	header := &seqproxyapi.ResponseHeader{Typing: docsTyping()}
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		ResponseType: &seqproxyapi.StreamSearchResponse_Header{Header: header},
	}); err != nil {
		return outcomeNone, status.Errorf(codes.Internal, "failed to send header: %v", err)
	}

	var batch []*seqproxyapi.Record
	for doc, err := range search.DocsIteratorSeq(sResp.docsStream) {
		if err != nil {
			return outcomeNone, err
		}
		batch = append(batch, docToRecord(doc))
		if len(batch) >= streamSearchBatchSize {
			if err := sendRecords(stream, batch); err != nil {
				return outcomeNone, err
			}
			batch = batch[:0]
			if outcome, stop := checkControl(controlCh, recvErrCh, ctx); stop {
				return outcome, nil
			}
		}
	}
	if len(batch) > 0 {
		if err := sendRecords(stream, batch); err != nil {
			return outcomeNone, err
		}
	}
	return outcomeNone, nil
}

// streamSearchAggs streams aggregation buckets as batches of records. Each
// record carries two columns: key (STRING) and value (FLOAT64).
func (g *grpcV1) streamSearchAggs(
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
	aggs []*seqproxyapi.AggQuery,
	sResp *proxySearchResponse,
	tr *querytracer.Tracer,
	controlCh <-chan *seqproxyapi.StreamControl,
	recvErrCh <-chan error,
	ctx context.Context,
) (controlOutcome, error) {
	header := &seqproxyapi.ResponseHeader{Typing: aggsTyping()}
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		ResponseType: &seqproxyapi.StreamSearchResponse_Header{Header: header},
	}); err != nil {
		return outcomeNone, status.Errorf(codes.Internal, "failed to send header: %v", err)
	}

	aggTr := tr.NewChild("aggregate")
	allAggregations := sResp.qpr.Aggregate(aggregationArgsFromProto(aggs))
	aggTr.Done()

	var batch []*seqproxyapi.Record
	for _, agg := range allAggregations {
		for _, item := range agg.Buckets {
			batch = append(batch, aggBucketToRecord(item))
			if len(batch) >= streamSearchBatchSize {
				if err := sendRecords(stream, batch); err != nil {
					return outcomeNone, err
				}
				batch = batch[:0]
				if outcome, stop := checkControl(controlCh, recvErrCh, ctx); stop {
					return outcome, nil
				}
			}
		}
	}
	if len(batch) > 0 {
		if err := sendRecords(stream, batch); err != nil {
			return outcomeNone, err
		}
	}
	return outcomeNone, nil
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

func docToRecord(doc search.StreamingDoc) *seqproxyapi.Record {
	return &seqproxyapi.Record{
		RawData: [][]byte{
			[]byte(doc.ID.String()),
			Uint64ToBytes(uint64(doc.ID.MID)),
			doc.Data,
		},
	}
}

func aggBucketToRecord(aggBucket seq.AggregationBucket) *seqproxyapi.Record {
	return &seqproxyapi.Record{
		RawData: [][]byte{
			[]byte(aggBucket.Name),
			Uint64ToBytes(math.Float64bits(aggBucket.Value)),
			Uint64ToBytes(uint64(aggBucket.MID)),
		},
	}
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

func buildProxyReq(q *seqproxyapi.StreamSearchQuery) (*seqproxyapi.ComplexSearchRequest, error) {
	seqql, err := parser.ParseSeqQL(q.Query, nil)
	if err != nil {
		return nil, err
	}

	proxyReq := &seqproxyapi.ComplexSearchRequest{
		Query: &seqproxyapi.SearchQuery{
			Query:   q.Query,
			From:    q.From,
			To:      q.To,
			Explain: q.Explain,
		},
		WithTotal: q.WithTotal,
		OffsetId:  q.OffsetId,
	}

	for _, pipe := range seqql.Pipes {
		switch p := pipe.(type) {
		case *parser.PipeLimit:
			proxyReq.Size = int64(p.Limit)
		case *parser.PipeOffset:
			proxyReq.Offset = int64(p.Offset)
		case *parser.PipeSort:
			order := seqproxyapi.Order_ORDER_DESC
			if p.Order == "asc" {
				order = seqproxyapi.Order_ORDER_ASC
			}
			proxyReq.Order = order
		case *parser.PipeStats:
			agg := p.Agg
			proxyReqAgg := &seqproxyapi.AggQuery{
				Field:     agg.Field,
				GroupBy:   agg.GroupBy,
				Func:      mustConvertStringToAggFunc(agg.Func),
				Quantiles: agg.Quantiles,
			}
			if agg.Interval != "" {
				proxyReqAgg.Interval = &agg.Interval
			}
			proxyReq.Aggs = append(proxyReq.Aggs, proxyReqAgg)
		default:
			continue
		}
	}

	return proxyReq, nil
}

func mustConvertStringToAggFunc(funcName string) seqproxyapi.AggFunc {
	switch funcName {
	case "count":
		return seqproxyapi.AggFunc_AGG_FUNC_COUNT
	case "sum":
		return seqproxyapi.AggFunc_AGG_FUNC_SUM
	case "min":
		return seqproxyapi.AggFunc_AGG_FUNC_MIN
	case "max":
		return seqproxyapi.AggFunc_AGG_FUNC_MAX
	case "avg":
		return seqproxyapi.AggFunc_AGG_FUNC_AVG
	case "quantile":
		return seqproxyapi.AggFunc_AGG_FUNC_QUANTILE
	case "unique":
		return seqproxyapi.AggFunc_AGG_FUNC_UNIQUE
	case "unique_count":
		return seqproxyapi.AggFunc_AGG_FUNC_UNIQUE_COUNT
	default:
		panic(fmt.Errorf("unknown aggregation function: %s", funcName))
	}
}

func Uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}
