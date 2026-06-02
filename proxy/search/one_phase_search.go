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
) (query.RecordProducer, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	var partialRespErr error

	streams, err := si.searchStoresOnePhase(ctx, sr, searchStores, tr)
	if err != nil {
		switch {
		case errors.Is(err, consts.ErrIngestorQueryWantsOldData):
			if len(si.config.ReadStores.Shards) == 0 {
				logger.Error("no cold stores, but hot mode is enabled, bad configuration of stores!")
				return nil, err
			}
			metric.SearchColdTotal.Inc()
			streams, err = si.searchStoresOnePhase(ctx, sr, si.config.ReadStores, tr)
			if err != nil {
				metric.SearchColdErrors.Add(1)
				if errors.Is(err, consts.ErrPartialResponse) {
					partialRespErr = err // consider partial response from cold stores as a result
				} else {
					// errors from both hot and cold stores, return error
					return nil, err
				}
			}
		case errors.Is(err, consts.ErrPartialResponse):
			partialRespErr = err // consider partial response from hot stores as a result
		default:
			// unexpected error on all hot replica sets (usually bad query)
			return nil, err
		}
	}

	var mergedStream query.RecordProducer

	hasAggs := len(sr.AggQ) > 0 // TODO: check based on stats pipe
	if hasAggs {
		mergedStream = exec.NewDistributedAggregator(streams, sr.AggQ[0].Func) // TODO: multiple aggs (???)
	} else {
		mergedDocsStream := exec.NewNMergedProducers(streams, 0, "", query.DataTypeUint64, execOrderFromSeqOrder(sr.Order))
		mergedStream = exec.NewLimiter(mergedDocsStream, uint32(sr.Size)) // TODO: offset
	}

	return mergedStream, partialRespErr
}

func (si *Ingestor) searchStoresOnePhase(
	ctx context.Context,
	sr *SearchRequest,
	s *stores.Stores,
	tr *querytracer.Tracer,
) ([]query.RecordProducer, error) {
	type ShardResponse struct {
		Stream query.RecordProducer
		Err    error
	}

	wg := sync.WaitGroup{}
	wg.Add(len(s.Shards))
	respChan := make(chan ShardResponse, len(s.Shards))
	for _, shard := range s.Shards {
		searchShardTr := tr.NewChild("proxy/searchShardOnePhase")
		go func(shard []string, tr *querytracer.Tracer) {
			defer wg.Done()
			defer tr.Done()

			stream, err := si.searchShardOnePhase(ctx, shard, sr, tr)
			respChan <- ShardResponse{
				Stream: stream,
				Err:    err,
			}
		}(shard, searchShardTr)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	streams := make([]query.RecordProducer, 0, len(s.Shards))
	var errs []error
	for resp := range respChan {
		if err := resp.Err; err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				// At least one hot store doesn't have such old data
				return nil, err
			}
			if errors.Is(err, consts.ErrTooManyFractionsHit) {
				return nil, err
			}
			errs = append(errs, err)
			continue
		}
		streams = append(streams, resp.Stream)
	}

	if err := util.DeduplicateErrors(errs); err != nil {
		if len(streams) != 0 {
			// There are errors, but some Shards returned data, so provide it to user
			return streams, fmt.Errorf("%w: %s", consts.ErrPartialResponse, err)
		}
		return nil, err
	}

	return streams, nil
}

func (si *Ingestor) searchShardOnePhase(
	ctx context.Context,
	hosts []string,
	request *SearchRequest,
	tr *querytracer.Tracer,
) (query.RecordProducer, error) {
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
		stream, err := si.searchHostOnePhase(ctx, hosts[i], request, tr)
		if err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				return nil, err
			}
			errs = append(errs, err)
			continue
		}
		return stream, nil
	}

	return nil, util.DeduplicateErrors(errs)
}

func (si *Ingestor) searchHostOnePhase(
	ctx context.Context,
	host string,
	request *SearchRequest,
	_ *querytracer.Tracer,
) (query.RecordProducer, error) {
	client, has := si.clients[host]
	if !has {
		return nil, fmt.Errorf("can't fetch: no client for host %s", host)
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
		return nil, fmt.Errorf("can't open stream: %s", err.Error())
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	header := msg.GetHeader()

	if header == nil {
		return nil, fmt.Errorf("can't read header")
	}

	// TODO: handle header.Metadata.Errors (do we really need it?)
	switch header.Metadata.Code {
	case storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA:
		return nil, fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData)
	case storeapi.SearchErrorCode_TOO_MANY_FIELD_TOKENS:
		return nil, fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyFieldTokens)
	case storeapi.SearchErrorCode_TOO_MANY_FIELD_VALUES:
		return nil, fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyFieldValues)
	case storeapi.SearchErrorCode_TOO_MANY_GROUP_TOKENS:
		return nil, fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyGroupTokens)
	case storeapi.SearchErrorCode_TOO_MANY_FRACTION_TOKENS:
		return nil, fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyFractionTokens)
	case storeapi.SearchErrorCode_MEMORY_LIMIT_EXCEEDED:
		return nil, fmt.Errorf("store forbids search request: %w", consts.ErrMemoryLimitExceeded)
	case storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT:
		return nil, fmt.Errorf("store forbids request: %w", consts.ErrTooManyFractionsHit)
	}

	return &OnePhaseSearchStreamIterator{typing: header.Typing, stream: stream}, nil
}

// TODO: move to executor (???)
type OnePhaseSearchStreamIterator struct {
	typing []*storeapi.Typing
	stream storeapi.StoreApi_OnePhaseSearchClient

	curBatch []*storeapi.Record
}

func (it *OnePhaseSearchStreamIterator) Next() (*query.Record, *query.Metadata) {
	if len(it.curBatch) == 0 {
		data, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, nil
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
