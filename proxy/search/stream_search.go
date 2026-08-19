package search

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type StreamSearchRequest struct {
	Query     string
	From      seq.MID
	To        seq.MID
	Explain   bool
	OffsetId  string
	WithTotal bool
	Agg       *AggQuery
	Order     seq.DocsOrder
	Size      int
	Offset    int
}

func (si *Ingestor) StreamSearch(
	ctx context.Context,
	sr *StreamSearchRequest,
	tr *querytracer.Tracer,
) (query.RecordProducer, ControlBroadcaster, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	var partialRespErr error

	streams, err := si.streamSearchStores(ctx, sr, searchStores, tr)
	if err != nil {
		switch {
		case errors.Is(err, consts.ErrIngestorQueryWantsOldData):
			if len(si.config.ReadStores.Shards) == 0 {
				logger.Error("no cold stores, but hot mode is enabled, bad configuration of stores!")
				return nil, nil, err
			}
			metric.SearchColdTotal.Inc()
			streams, err = si.streamSearchStores(ctx, sr, si.config.ReadStores, tr)
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

	broadcaster := newControlBroadcaster(streams)
	producers := make([]query.RecordProducer, 0, len(streams))
	for _, s := range streams {
		producers = append(producers, s)
	}

	var mergedStream query.RecordProducer
	if sr.Agg != nil {
		mergedStream = exec.NewDistributedAggregator(producers, sr.Agg.Func, sr.Agg.Quantiles)
	} else {
		const seqIdColIdx = 0
		mergedDocsStream := exec.NewNMergedProducers(producers, seqIdColIdx, "", query.DataTypeSeqID, sr.Order)
		mergedStream = exec.NewLimiter(mergedDocsStream, uint32(sr.Size), uint32(sr.Offset))
	}

	return mergedStream, broadcaster, partialRespErr
}

func (si *Ingestor) streamSearchStores(
	ctx context.Context,
	sr *StreamSearchRequest,
	s *stores.Stores,
	tr *querytracer.Tracer,
) ([]*StreamSearchIterator, error) {
	type ShardResponse struct {
		Stream *StreamSearchIterator
		Err    error
	}

	wg := sync.WaitGroup{}
	wg.Add(len(s.Shards))
	respChan := make(chan ShardResponse, len(s.Shards))
	for _, shard := range s.Shards {
		searchShardTr := tr.NewChild("proxy/streamSearchShard")
		go func(shard []string, tr *querytracer.Tracer) {
			defer wg.Done()
			stream, err := si.streamSearchShard(ctx, shard, sr, tr)
			if err != nil {
				searchShardTr.Done()
			}
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

	// earlyErr is set when a fail-fast error (old data / too many fractions hit) is observed.
	// In this case we must collect and close all the streams before returning.
	var earlyErr error

	streams := make([]*StreamSearchIterator, 0, len(s.Shards))
	var errs []error
	for resp := range respChan {
		if err := resp.Err; err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) || errors.Is(err, consts.ErrTooManyFractionsHit) {
				if earlyErr == nil {
					earlyErr = err
				}
				if resp.Stream != nil {
					_ = resp.Stream.Close()
				}
				continue
			}
			errs = append(errs, err)
			continue
		}
		streams = append(streams, resp.Stream)
	}

	if earlyErr != nil {
		closeStreams(streams)
		return nil, earlyErr
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

func (si *Ingestor) streamSearchShard(
	ctx context.Context,
	hosts []string,
	request *StreamSearchRequest,
	tr *querytracer.Tracer,
) (*StreamSearchIterator, error) {
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
		stream, err := si.streamSearchHost(ctx, host, request, tr)
		if err != nil {
			tr.Printf("got error from host: %s", err)
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

func (si *Ingestor) streamSearchHost(
	ctx context.Context,
	host string,
	request *StreamSearchRequest,
	tr *querytracer.Tracer,
) (*StreamSearchIterator, error) {
	client, has := si.clients[host]
	if !has {
		return nil, fmt.Errorf("can't fetch: no client for host %s", host)
	}

	req := &storeapi.StreamSearchRequest{
		RequestType: &storeapi.StreamSearchRequest_Query{
			Query: &storeapi.StreamSearchQuery{
				Query:     request.Query,
				From:      timestamppb.New(request.From.Time()),
				To:        timestamppb.New(request.To.Time()),
				Explain:   request.Explain,
				OffsetId:  request.OffsetId,
				WithTotal: request.WithTotal,
			},
		},
	}

	stream, err := client.StreamSearch(ctx,
		grpc.MaxCallRecvMsgSize(256*int(units.MiB)),
		grpc.MaxCallSendMsgSize(256*int(units.MiB)),
	)
	if err != nil {
		return nil, fmt.Errorf("can't open stream: %s", err.Error())
	}

	err = stream.Send(req)
	if err != nil {
		return nil, fmt.Errorf("can't send stream request: %s", err.Error())
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	switch v := msg.ResponseType.(type) {
	case *storeapi.StreamSearchResponse_Header:
		return NewStreamSearchIterator(tr, v.Header, stream)
	case *storeapi.StreamSearchResponse_Summary:
		// The store refused the request before sending any data.
		if s := v.Summary; s != nil && s.Error != nil {
			return nil, storeCodeToError(s.Error.Code)
		}
		return nil, fmt.Errorf("can't read header: store sent summary without data")
	default:
		return nil, fmt.Errorf("can't read header")
	}
}

func closeStreams(streams []*StreamSearchIterator) {
	for _, s := range streams {
		_ = s.Close()
	}
}

// NewStreamSearchIterator reads one message ahead after the header so that a
// summary-with-error sent immediately after the header (before any data) is
// detected on the open-stream phase and can trigger fail-fast in the
// ingestor. The prefetched message is buffered in the iterator.
func NewStreamSearchIterator(
	tr *querytracer.Tracer,
	header *storeapi.ResponseHeader,
	stream storeapi.StoreApi_StreamSearchClient,
) (*StreamSearchIterator, error) {
	it := &StreamSearchIterator{tr: tr, typing: header.Typing, stream: stream}

	msg, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		// No data and no summary: an empty result, not an error.
		return it, nil
	}
	if err != nil {
		return nil, err
	}
	if err := it.push(msg); err != nil {
		return nil, err
	}
	return it, nil
}

type StreamSearchIterator struct {
	tr *querytracer.Tracer

	typing []*storeapi.Typing
	stream storeapi.StoreApi_StreamSearchClient

	curBatch []*storeapi.Record

	total uint64
	err   error
	done  bool
}

func (it *StreamSearchIterator) Next() *query.Record {
	if it.done {
		return nil
	}

	if len(it.curBatch) == 0 {
		data, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			it.done = true
			return nil
		}
		if err != nil {
			it.err = err
			it.done = true
			return nil
		}
		if err := it.push(data); err != nil {
			it.err = err
			it.done = true
			return nil
		}
		if it.done {
			return nil
		}
	}

	record := it.curBatch[0]
	it.curBatch = it.curBatch[1:]

	recordVals := make([]*query.RecordVals, 0, len(record.RawData))
	for i, rawData := range record.RawData {
		recordVals = append(recordVals, query.NewRecordVals(query.DataType(it.typing[i].Type), rawData))
	}
	return query.NewRecord(recordVals)
}

// push handles a single message received from the store stream.
func (it *StreamSearchIterator) push(msg *storeapi.StreamSearchResponse) error {
	switch v := msg.ResponseType.(type) {
	case *storeapi.StreamSearchResponse_Header:
		// header should be sent only once
		return errors.New("unexpected header message")
	case *storeapi.StreamSearchResponse_Data:
		it.curBatch = v.Data.GetBatch().GetRecords()
	case *storeapi.StreamSearchResponse_Summary:
		it.total = v.Summary.Total
		if v.Summary.Explain != nil {
			it.tr.AddChildWithSpan(explainEntryToTracerSpan(v.Summary.Explain))
		}
		if v.Summary.Error != nil {
			it.err = storeCodeToError(v.Summary.Error.Code)
		}
		it.done = true
	}
	return nil
}

// SendControl forwards a control action to the store. It is safe to call concurrently with Recv/Next.
// Errors (e.g. the store already closed the stream) are best-effort: the caller proceeds regardless.
func (it *StreamSearchIterator) SendControl(action storeapi.ControlAction) error {
	return it.stream.Send(&storeapi.StreamSearchRequest{
		RequestType: &storeapi.StreamSearchRequest_Control{
			Control: &storeapi.StreamControl{Action: action},
		},
	})
}

// Close releases the store stream when the iterator is discarded without being finalized.
// It is best-effort and safe to call on an already-closed stream; it must not be called concurrently with Next/Finalize.
func (it *StreamSearchIterator) Close() error {
	_ = it.SendControl(storeapi.ControlAction_CANCEL)
	err := it.stream.CloseSend()
	it.tr.Done()
	return err
}

func (it *StreamSearchIterator) Finalize() *query.Summary {
	// If the stream was finalized before the data was exhausted, the store's summary may still be in flight.
	// Drain the remaining messages so the store-reported summary is not lost.
	if !it.done {
		it.drain()
	}
	it.tr.Done()
	return &query.Summary{Total: it.total, Err: it.err}
}

// drain reads the store stream until the summary message (or EOF/error) is
// received, capturing the store-reported total and error.
// It must be called only after the producer has stopped calling Next concurrently.
func (it *StreamSearchIterator) drain() {
	for !it.done {
		msg, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			it.done = true
			return
		}
		if err != nil {
			it.err = err
			it.done = true
			return
		}
		if err := it.push(msg); err != nil {
			it.err = err
			it.done = true
			return
		}
	}
}

// ControlBroadcaster fans a control action out to every store stream backing a search.
type ControlBroadcaster interface {
	SendControl(storeapi.ControlAction)
}

type controlBroadcaster struct {
	streams []*StreamSearchIterator
}

func newControlBroadcaster(streams []*StreamSearchIterator) ControlBroadcaster {
	return &controlBroadcaster{streams: streams}
}

func (b *controlBroadcaster) SendControl(action storeapi.ControlAction) {
	for _, s := range b.streams {
		// Best-effort: a store that already terminated the stream returns an
		// error here, which we intentionally ignore.
		_ = s.SendControl(action)
	}
}

func storeCodeToError(code storeapi.SearchErrorCode) error {
	switch code {
	case storeapi.SearchErrorCode_NO_ERROR:
		return nil
	case storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA:
		return fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData)
	case storeapi.SearchErrorCode_TOO_MANY_FIELD_TOKENS:
		return fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyFieldTokens)
	case storeapi.SearchErrorCode_TOO_MANY_FIELD_VALUES:
		return fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyFieldValues)
	case storeapi.SearchErrorCode_TOO_MANY_GROUP_TOKENS:
		return fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyGroupTokens)
	case storeapi.SearchErrorCode_TOO_MANY_FRACTION_TOKENS:
		return fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyFractionTokens)
	case storeapi.SearchErrorCode_MEMORY_LIMIT_EXCEEDED:
		return fmt.Errorf("store forbids search request: %w", consts.ErrMemoryLimitExceeded)
	case storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT:
		return fmt.Errorf("store forbids request: %w", consts.ErrTooManyFractionsHit)
	default:
		return fmt.Errorf("unknown store error code: %s", code.String())
	}
}
