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
) (*seq.QPR, query.RecordProducer, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	var partialRespErr error

	qprs, streams, err := si.searchStoresOnePhase(ctx, sr, searchStores, tr)
	if err != nil {
		switch {
		case errors.Is(err, consts.ErrIngestorQueryWantsOldData):
			if len(si.config.ReadStores.Shards) == 0 {
				logger.Error("no cold stores, but hot mode is enabled, bad configuration of stores!")
				return nil, nil, err
			}
			metric.SearchColdTotal.Inc()
			qprs, streams, err = si.searchStoresOnePhase(ctx, sr, si.config.ReadStores, tr)
			if err != nil {
				metric.SearchColdErrors.Add(1)
				if errors.Is(err, consts.ErrPartialResponse) {
					partialRespErr = err // consider partial response from cold stores as a result
				} else {
					// errors from both hot and cold stores, return error
					return nil, nil, err
				}
			}
		case errors.Is(err, consts.ErrPartialResponse):
			partialRespErr = err // consider partial response from hot stores as a result
		default:
			// unexpected error on all hot replica sets (usually bad query)
			return nil, nil, err
		}
	}

	// TODO: do we really need QPR (???)
	resQpr := &seq.QPR{
		Histogram: make(map[seq.MID]uint64),
		Aggs:      make([]seq.AggregatableSamples, len(sr.AggQ)),
	}
	seq.MergeQPRs(resQpr, qprs, sr.Offset+sr.Size, sr.Interval, sr.Order)

	var mergedStream query.RecordProducer

	hasAggs := len(sr.AggQ) > 0 // TODO: check based on stats pipe
	if hasAggs {
		mergedStream = exec.NewDistributedAggregator(streams, sr.AggQ[0].Func) // TODO: multiple aggs (???)
	} else {
		mergedDocsStream := exec.NewNMergedProducers(streams, 0, "", query.DataTypeUint64, execOrderFromSeqOrder(sr.Order))
		mergedStream = exec.NewLimiter(mergedDocsStream, uint32(sr.Size)) // TODO: offset
	}

	return resQpr, mergedStream, partialRespErr
}

func (si *Ingestor) searchStoresOnePhase(
	ctx context.Context,
	sr *SearchRequest,
	s *stores.Stores,
	tr *querytracer.Tracer,
) ([]*seq.QPR, []query.RecordProducer, error) {
	type ShardResponse struct {
		QPR    *seq.QPR
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

			qpr, stream, err := si.searchShardOnePhase(ctx, shard, sr, tr)
			respChan <- ShardResponse{
				QPR:    qpr,
				Stream: stream,
				Err:    err,
			}
		}(shard, searchShardTr)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	qprs := make([]*seq.QPR, 0, len(s.Shards))
	streams := make([]query.RecordProducer, 0, len(s.Shards))
	var errs []error
	for resp := range respChan {
		if err := resp.Err; err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				// At least one hot store doesn't have such old data
				return nil, nil, err
			}
			if errors.Is(err, consts.ErrTooManyFractionsHit) {
				return nil, nil, err
			}
			errs = append(errs, err)
			continue
		}

		qprs = append(qprs, resp.QPR)
		streams = append(streams, resp.Stream)
	}

	if err := util.DeduplicateErrors(errs); err != nil {
		if len(qprs) != 0 {
			// There are errors, but some Shards returned data, so provide it to user
			return qprs, streams, fmt.Errorf("%w: %s", consts.ErrPartialResponse, err)
		}
		return nil, nil, err
	}

	return qprs, streams, nil
}

func (si *Ingestor) searchShardOnePhase(
	ctx context.Context,
	hosts []string,
	request *SearchRequest,
	tr *querytracer.Tracer,
) (*seq.QPR, query.RecordProducer, error) {
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
		qpr, stream, err := si.searchHostOnePhase(ctx, hosts[i], request, tr)
		if err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				return nil, nil, err
			}
			errs = append(errs, err)
			continue
		}
		return qpr, stream, nil
	}

	return nil, nil, util.DeduplicateErrors(errs)
}

func (si *Ingestor) searchHostOnePhase(
	ctx context.Context,
	host string,
	request *SearchRequest,
	_ *querytracer.Tracer,
) (*seq.QPR, query.RecordProducer, error) {
	client, has := si.clients[host]
	if !has {
		return nil, nil, fmt.Errorf("can't fetch: no client for host %s", host)
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
		return nil, nil, fmt.Errorf("can't open stream: %s", err.Error())
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}

	header := msg.GetHeader()

	if header == nil {
		return nil, nil, fmt.Errorf("can't read header")
	}

	if header.Metadata.Code == storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA {
		return nil, nil, fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData)
	}

	errs := make([]seq.ErrorSource, 0, len(header.Metadata.Errors))
	for _, err := range header.Metadata.Errors {
		errs = append(errs, seq.ErrorSource{ErrStr: err})
	}

	qpr := &seq.QPR{
		Total:  header.Metadata.Total,
		Errors: errs,
	}

	return qpr, &OnePhaseSearchStreamIterator{typing: header.Typing, stream: stream}, nil
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
