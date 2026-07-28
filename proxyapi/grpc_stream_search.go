package proxyapi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
const streamSearchBatchSize = 100

func (g *grpcV1) StreamSearch(
	req *seqproxyapi.StreamSearchRequest,
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
) error {
	// TODO: do we need cancel by timeout in streams (???)
	ctx, cancel := context.WithTimeout(stream.Context(), g.config.SearchTimeout)
	defer cancel()

	q := req.GetQuery()
	if q == nil {
		return errors.New("no query") // TODO:
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
	sResp, err := g.doSearch(ctx, proxyReq, true, tr)
	if err != nil {
		return err
	}
	if sResp.err != nil && sResp.err.Code == seqproxyapi.ErrorCode_ERROR_CODE_PARTIAL_RESPONSE && shouldFailPartialResponse(ctx) {
		return status.Error(codes.Internal, "partial response: not all shards returned results")
	}
	if sResp.err != nil && !shouldHaveResponse(sResp.err.Code) {
		return errors.New(sResp.err.Message)
	}

	if len(proxyReq.Aggs) > 0 {
		if err := g.streamSearchAggs(stream, proxyReq.Aggs, sResp, tr); err != nil {
			return err
		}
	} else {
		if err := g.streamSearchDocs(stream, sResp); err != nil {
			return err
		}
	}

	summary := &seqproxyapi.ResponseSummary{Total: sResp.qpr.Total}
	if sResp.err != nil {
		summary.Error = sResp.err
	} else {
		summary.Error = &seqproxyapi.Error{Code: seqproxyapi.ErrorCode_ERROR_CODE_NO}
	}
	tr.Done()
	summary.Explain = tracerSpanToExplainEntry(tr.ToSpan())
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		RequestType: &seqproxyapi.StreamSearchResponse_Summary{Summary: summary},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send summary: %v", err)
	}

	return nil
}

// streamSearchDocs streams matched documents as batches of records. Each record
// carries three columns: id (SEQ_ID), time (UINT64 nanoseconds) and data (RAW_DOCUMENT).
func (g *grpcV1) streamSearchDocs(
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
	sResp *proxySearchResponse,
) error {
	header := &seqproxyapi.ResponseHeader{Typing: docsTyping()}
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		RequestType: &seqproxyapi.StreamSearchResponse_Header{Header: header},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send header: %v", err)
	}

	var batch []*seqproxyapi.Record
	for doc, err := sResp.docsStream.Next(); err == nil; doc, err = sResp.docsStream.Next() {
		batch = append(batch, docToRecord(doc))
		if len(batch) >= streamSearchBatchSize {
			if err := sendRecords(stream, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := sendRecords(stream, batch); err != nil {
			return err
		}
	}
	return nil
}

// streamSearchAggs streams aggregation buckets as batches of records. Each
// record carries two columns: key (STRING) and value (FLOAT64).
func (g *grpcV1) streamSearchAggs(
	stream seqproxyapi.SeqProxyApi_StreamSearchServer,
	aggs []*seqproxyapi.AggQuery,
	sResp *proxySearchResponse,
	tr *querytracer.Tracer,
) error {
	header := &seqproxyapi.ResponseHeader{Typing: aggsTyping()}
	if err := stream.Send(&seqproxyapi.StreamSearchResponse{
		RequestType: &seqproxyapi.StreamSearchResponse_Header{Header: header},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send header: %v", err)
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
					return err
				}
				batch = batch[:0]
			}
		}
	}
	if len(batch) > 0 {
		if err := sendRecords(stream, batch); err != nil {
			return err
		}
	}
	return nil
}

func sendRecords(stream seqproxyapi.SeqProxyApi_StreamSearchServer, records []*seqproxyapi.Record) error {
	resp := &seqproxyapi.StreamSearchResponse{
		RequestType: &seqproxyapi.StreamSearchResponse_Data{Data: &seqproxyapi.ResponseData{
			Batch: &seqproxyapi.RecordsBatch{Records: records},
		}},
	}
	if err := stream.Send(resp); err != nil {
		return status.Errorf(codes.Internal, "failed to send data: %v", err)
	}
	return nil
}

func docToRecord(doc search.StreamingDoc) *seqproxyapi.Record {
	timeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBuf, uint64(doc.ID.MID))
	return &seqproxyapi.Record{
		RawData: [][]byte{
			[]byte(doc.ID.String()),
			timeBuf,
			doc.Data,
		},
	}
}

func aggBucketToRecord(item seq.AggregationBucket) *seqproxyapi.Record {
	valueBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBuf, math.Float64bits(item.Value))
	return &seqproxyapi.Record{
		RawData: [][]byte{
			[]byte(item.Name),
			valueBuf,
		},
	}
}

func docsTyping() []*seqproxyapi.Typing {
	return []*seqproxyapi.Typing{
		{Title: "id", Type: seqproxyapi.DataType_SEQ_ID},
		{Title: "time", Type: seqproxyapi.DataType_UINT64},
		{Title: "data", Type: seqproxyapi.DataType_RAW_DOCUMENT},
	}
}

func aggsTyping() []*seqproxyapi.Typing {
	return []*seqproxyapi.Typing{
		{Title: "key", Type: seqproxyapi.DataType_STRING},
		{Title: "value", Type: seqproxyapi.DataType_FLOAT64},
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
			proxyReq.Size = int64(p.Offset)
		case *parser.PipeSort:
			order := seqproxyapi.Order_ORDER_DESC
			if p.Order == "asc" {
				order = seqproxyapi.Order_ORDER_ASC
			}
			proxyReq.Order = order
		case *parser.PipeStats:
			for _, agg := range p.Aggs {
				proxyReqAgg := &seqproxyapi.AggQuery{
					Field:     agg.Field,
					GroupBy:   agg.GroupBy,
					Func:      mustConvertStringToAggFunc(agg.Func), // TODO: ???
					Quantiles: agg.Quantiles,
				}
				if agg.Interval != "" {
					proxyReqAgg.Interval = &agg.Interval
				}
				proxyReq.Aggs = append(proxyReq.Aggs, proxyReqAgg)
			}
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
