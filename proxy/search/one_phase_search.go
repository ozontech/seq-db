package search

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/alecthomas/units"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/exec"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

func (si *Ingestor) OnePhaseSearch(
	ctx context.Context,
	sr *SearchRequest,
	tr *querytracer.Tracer,
) (*seq.QPR, query.RecordProducer, AggsIterator, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	var partialRespErr error

	qprs, docs, aggs, err := si.searchStoresOnePhase(ctx, sr, searchStores, tr)
	if err != nil {
		switch {
		case errors.Is(err, consts.ErrIngestorQueryWantsOldData):
			if len(si.config.ReadStores.Shards) == 0 {
				logger.Error("no cold stores, but hot mode is enabled, bad configuration of stores!")
				return nil, nil, nil, err
			}
			metric.SearchColdTotal.Inc()
			qprs, docs, aggs, err = si.searchStoresOnePhase(ctx, sr, si.config.ReadStores, tr)
			if err != nil {
				metric.SearchColdErrors.Add(1)
				if errors.Is(err, consts.ErrPartialResponse) {
					partialRespErr = err // consider partial response from cold stores as a result
				} else {
					// errors from both hot and cold stores, return error
					return nil, nil, nil, err
				}
			}
		case errors.Is(err, consts.ErrPartialResponse):
			partialRespErr = err // consider partial response from hot stores as a result
		default:
			// unexpected error on all hot replica sets (usually bad query)
			return nil, nil, nil, err
		}
	}

	// TODO: do we really need QPR (???)
	resQpr := &seq.QPR{
		Histogram: make(map[seq.MID]uint64),
		Aggs:      make([]seq.AggregatableSamples, len(sr.AggQ)),
	}
	seq.MergeQPRs(resQpr, qprs, sr.Offset+sr.Size, sr.Interval, sr.Order)

	mergedDocsStream := exec.NewNMergedProducers(docs, 0, "", query.DataTypeUint64, execOrderFromSeqOrder(sr.Order))
	limiter := exec.NewLimiter(mergedDocsStream, uint32(sr.Size)) // TODO: offset

	return resQpr, limiter, aggs[0], partialRespErr
}

func (si *Ingestor) searchStoresOnePhase(
	ctx context.Context,
	sr *SearchRequest,
	s *stores.Stores,
	tr *querytracer.Tracer,
) ([]*seq.QPR, []query.RecordProducer, []AggsIterator, error) {
	type ShardResponse struct {
		QPR  *seq.QPR
		Docs query.RecordProducer
		Aggs AggsIterator
		Err  error
	}

	wg := sync.WaitGroup{}
	wg.Add(len(s.Shards))
	respChan := make(chan ShardResponse, len(s.Shards))
	for _, shard := range s.Shards {
		searchShardTr := tr.NewChild("proxy/searchShardOnePhase")
		go func(shard []string, tr *querytracer.Tracer) {
			defer wg.Done()
			defer tr.Done()

			qpr, docs, aggs, err := si.searchShardOnePhase(ctx, shard, sr, tr)
			respChan <- ShardResponse{
				QPR:  qpr,
				Docs: docs,
				Aggs: aggs,
				Err:  err,
			}
		}(shard, searchShardTr)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	qprs := make([]*seq.QPR, 0, len(s.Shards))
	docs := make([]query.RecordProducer, 0, len(s.Shards))
	aggs := make([]AggsIterator, 0, len(s.Shards))
	var errs []error
	for resp := range respChan {
		if err := resp.Err; err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				// At least one hot store doesn't have such old data
				return nil, nil, nil, err
			}
			if errors.Is(err, consts.ErrTooManyFractionsHit) {
				return nil, nil, nil, err
			}
			errs = append(errs, err)
			continue
		}

		qprs = append(qprs, resp.QPR)
		docs = append(docs, resp.Docs)
		aggs = append(aggs, resp.Aggs)
	}

	if err := util.DeduplicateErrors(errs); err != nil {
		if len(qprs) != 0 {
			// There are errors, but some Shards returned data, so provide it to user
			return qprs, docs, aggs, fmt.Errorf("%w: %s", consts.ErrPartialResponse, err)
		}
		return nil, nil, nil, err
	}

	return qprs, docs, aggs, nil
}

func (si *Ingestor) searchShardOnePhase(
	ctx context.Context,
	hosts []string,
	request *SearchRequest,
	tr *querytracer.Tracer,
) (*seq.QPR, query.RecordProducer, AggsIterator, error) {
	var idx []int
	if si.config.ShuffleReplicas {
		idx = util.IdxShuffle(len(hosts))
	} else {
		idx = util.IdxFill(len(hosts))
	}

	var errs []error
	for i := range len(hosts) {
		host := hosts[idx[i]]
		tr.Printf("Making search request to %s", host)
		qpr, docs, aggs, err := si.searchHostOnePhase(ctx, hosts[i], request, tr) // TODO:
		if err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				return nil, nil, nil, err
			}
			errs = append(errs, err)
			continue
		}
		return qpr, docs, aggs, nil
	}

	return nil, nil, nil, util.DeduplicateErrors(errs)
}

func (si *Ingestor) searchHostOnePhase(
	ctx context.Context,
	host string,
	request *SearchRequest,
	_ *querytracer.Tracer,
) (*seq.QPR, query.RecordProducer, AggsIterator, error) {
	client, has := si.clients[host]
	if !has {
		return nil, nil, nil, fmt.Errorf("can't fetch: no client for host %s", host)
	}

	fieldsFilter := tryParseFieldsFilter(string(request.Q))
	req := &storeapi.OnePhaseSearchRequest{
		Query:     string(request.Q),
		From:      timestamppb.New(request.From.Time()),
		To:        timestamppb.New(request.To.Time()),
		Size:      int64(request.Size),
		Offset:    int64(request.Offset),
		Explain:   request.Explain,
		WithTotal: request.WithTotal,
		Order:     storeapi.Order(request.Order),
		OffsetId:  request.OffsetId,
		FieldsFilter: &storeapi.FieldsFilter{
			Fields:    fieldsFilter.Fields,
			AllowList: fieldsFilter.AllowList,
		},
	}

	stream, err := client.OnePhaseSearch(ctx, req,
		grpc.MaxCallRecvMsgSize(256*int(units.MiB)),
		grpc.MaxCallSendMsgSize(256*int(units.MiB)),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("can't open stream: %s", err.Error())
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, nil, nil, nil
	}

	header := msg.GetHeader()

	if header == nil {
		return nil, nil, nil, fmt.Errorf("can't read header")
	}

	if header.Metadata.Code == storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA {
		return nil, nil, nil, fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData)
	}

	errs := make([]seq.ErrorSource, 0, len(header.Metadata.Errors))
	for _, err := range header.Metadata.Errors {
		errs = append(errs, seq.ErrorSource{ErrStr: err})
	}

	qpr := &seq.QPR{
		Total:  header.Metadata.Total,
		Errors: errs,
	}

	return qpr,
		&OnePhaseSearchDocsIterator{typing: header.Typing, stream: stream},
		&OnePhaseSearchAggsIterator{stream: stream, limit: request.Size},
		nil
}

// TODO: move to executor (???)
type OnePhaseSearchDocsIterator struct {
	typing []*storeapi.Typing
	stream storeapi.StoreApi_OnePhaseSearchClient

	curBatch []*storeapi.Record
}

func (it *OnePhaseSearchDocsIterator) Next() (*query.Record, *query.Metadata) {
	if len(it.curBatch) == 0 {
		data, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, &query.Metadata{Err: io.EOF}
		}
		if err != nil {
			return nil, &query.Metadata{Err: err}
		}
		it.curBatch = data.GetBatch().Records
	}

	record := it.curBatch[0]
	it.curBatch = it.curBatch[1:]

	recordVals := make([]*query.RecordVals, 0, len(record.RawData))
	for i, rawData := range record.RawData {
		recordVals = append(recordVals, query.NewRecordVals(query.DataType(it.typing[i].Type), rawData))
	}
	return query.NewRecord(recordVals), nil
}

type StreamingAgg struct {
	Label     string
	Min       float64
	Max       float64
	Sum       float64
	Total     uint64
	NotExists uint64
}

type AggsIterator interface {
	Next() (StreamingAgg, error)
}

type OnePhaseSearchAggsIterator struct {
	stream storeapi.StoreApi_OnePhaseSearchClient

	curBatch []*storeapi.Record

	fetched int
	limit   int
}

func (it *OnePhaseSearchAggsIterator) Next() (StreamingAgg, error) {
	if it.fetched >= it.limit {
		return StreamingAgg{}, io.EOF
	}

	if len(it.curBatch) == 0 {
		data, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			return StreamingAgg{}, io.EOF
		}
		if err != nil {
			return StreamingAgg{}, err
		}
		it.curBatch = data.GetBatch().Records
	}

	// TODO: get fields values from columns info

	record := it.curBatch[0]
	it.curBatch = it.curBatch[1:]

	it.fetched++

	return StreamingAgg{
		Label:     string(record.RawData[0]),
		Min:       Float64FromBytes(record.RawData[1]),
		Max:       Float64FromBytes(record.RawData[2]),
		Sum:       Float64FromBytes(record.RawData[3]),
		Total:     binary.LittleEndian.Uint64(record.RawData[4]),
		NotExists: binary.LittleEndian.Uint64(record.RawData[5]),
	}, nil
}

func Float64FromBytes(in []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(in))
}

func execOrderFromSeqOrder(in seq.DocsOrder) exec.Order {
	switch in {
	case seq.DocsOrderAsc:
		return exec.OrderAsc
	case seq.DocsOrderDesc:
		return exec.OrderDesc
	default:
		panic("unexpected")
	}
}
