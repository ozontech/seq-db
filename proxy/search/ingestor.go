package search

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Config struct {
	HotStores       *stores.Stores
	HotReadStores   *stores.Stores
	ReadStores      *stores.Stores
	WriteStores     *stores.Stores
	ShuffleReplicas bool
	MirrorAddr      string
}

type Ingestor struct {
	config         Config
	clients        map[string]storeapi.StoreApiClient
	sourceByClient map[string]uint64
	clientBySource map[uint64]string
}

func NewIngestor(config Config, clients map[string]storeapi.StoreApiClient) *Ingestor {
	sourceByClient := make(map[string]uint64, len(clients))
	clientBySource := make(map[uint64]string, len(clients))
	var index uint64
	for client := range clients {
		sourceByClient[client] = index
		clientBySource[index] = client
		index++
	}

	return &Ingestor{
		config:         config,
		clients:        clients,
		sourceByClient: sourceByClient,
		clientBySource: clientBySource,
	}
}

func (si *Ingestor) Search(ctx context.Context, sr *SearchRequest) (qpr *seq.QPR, docsStream DocsIterator, overallDuration time.Duration, err error) {
	if sr.Explain {
		logger.Info("search request",
			zap.String("query", string(sr.Q)),
			zap.String("from", sr.From.String()),
			zap.String("to", sr.To.String()),
		)
	}
	if sr.Size < 0 || sr.Offset < 0 {
		return nil, nil, 0, fmt.Errorf("%w: negative size or offset", consts.ErrInvalidArgument)
	}

	startTime := time.Now()
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}
	qprs, err := si.searchStores(ctx, sr, searchStores)
	var partialRespErr error

	if err != nil {
		switch {
		case errors.Is(err, consts.ErrIngestorQueryWantsOldData):
			if len(si.config.ReadStores.Shards) == 0 {
				logger.Error("no cold stores, but hot mode is enabled, bad configuration of stores!")
				return nil, nil, 0, err
			}
			metric.SearchColdTotal.Inc()
			qprs, err = si.searchStores(ctx, sr, si.config.ReadStores)
			if err != nil {
				metric.SearchColdErrors.Add(1)
				if errors.Is(err, consts.ErrPartialResponse) {
					partialRespErr = err // consider partial response from cold stores as a result
				} else {
					// errors from both hot and cold stores, return error
					return nil, nil, 0, err
				}
			}
		case errors.Is(err, consts.ErrPartialResponse):
			partialRespErr = err // consider partial response from hot stores as a result
		default:
			// unexpected error on all hot replica sets (usually bad query)
			return nil, nil, 0, err
		}
	}

	queryDuration := time.Since(startTime)

	t := time.Now()
	qpr = &seq.QPR{
		Histogram: make(map[seq.MID]uint64),
		Aggs:      make([]seq.QPRHistogram, len(sr.AggQ)),
	}
	seq.MergeQPRs(qpr, qprs, sr.Offset+sr.Size, sr.Interval, sr.Order)
	mergeDuration := time.Since(t)
	if len(qpr.Errors) > 0 {
		for _, errSource := range qpr.Errors {
			host := si.clientBySource[errSource.Source]
			logger.Info("store failed",
				zap.String("store", host),
				zap.String("query", string(sr.Q)),
				zap.String("error", errSource.ErrStr),
			)
		}
	}

	var size int
	qpr.IDs, size = si.paginateIDs(qpr.IDs, sr.Offset, sr.Size)
	ids := qpr.IDs

	t = time.Now()
	docsStream = EmptyDocsStream{}
	if sr.ShouldFetch && size > 0 {
		if util.IsCancelled(ctx) {
			return nil, nil, 0, ctx.Err()
		}
		metric.DocumentsRequested.Observe(float64(len(ids)))
		docsStream, err = si.FetchDocsStream(ctx, ids, sr.Explain)
		if err != nil {
			return nil, nil, 0, err
		}
	}

	fetchDuration := time.Since(t)
	overallDuration = time.Since(startTime)

	if sr.Explain {
		logger.Info("data", zap.Any("histogram", qpr.Histogram))
		logger.Info("search result",
			zap.Uint64("total", qpr.Total),
			zap.Int("fetched", len(ids)),
			zap.Int("buckets", len(qpr.Histogram)),
			util.ZapDurationWithPrec("query_ms", queryDuration, "ms", 2),
			util.ZapDurationWithPrec("merge_ms", mergeDuration, "ms", 2),
			util.ZapDurationWithPrec("fetch_ms", fetchDuration, "ms", 2),
			util.ZapDurationWithPrec("all_ms", overallDuration, "ms", 2),
		)
	}

	return qpr, docsStream, overallDuration, partialRespErr
}

func (si *Ingestor) paginateIDs(ids seq.IDSources, offset, size int) (seq.IDSources, int) {
	if len(ids) > offset {
		ids = ids[offset:]
	} else {
		ids = ids[:0]
	}

	if len(ids) > size {
		ids = ids[:size]
	} else {
		size = len(ids)
	}
	return ids, size
}

func (si *Ingestor) singleDocsStream(ctx context.Context, explain bool, source uint64, ids []seq.IDSource) (DocsIterator, error) {
	startTime := time.Now()
	host, has := si.clientBySource[source]
	if !has {
		return nil, fmt.Errorf("can't fetch: no host for source %q", source)
	}
	client, has := si.clients[host]
	if !has {
		return nil, fmt.Errorf("can't fetch: no client for host %s", host)
	}

	stream, err := client.Fetch(ctx, si.makeFetchReq(ids, explain),
		grpc.MaxCallRecvMsgSize(256*consts.MB),
		grpc.MaxCallSendMsgSize(256*consts.MB),
	)
	if err != nil {
		return nil, fmt.Errorf("can't fetch docs: %s", err.Error())
	}

	var it DocsIterator = newGrpcStreamIterator(stream, host, source, len(ids))
	if explain {
		it = newExplainWrapperIterator(it, ids, host, startTime)
	}

	return it, nil
}

func groupIDsBySource(ids []seq.IDSource) map[uint64][]seq.IDSource {
	hostsIDs := make(map[uint64][]seq.IDSource)
	for _, idSource := range ids {
		hostsIDs[idSource.Source] = append(hostsIDs[idSource.Source], idSource)
	}
	return hostsIDs
}

// lessFuncPosBased returns compare function based on initial order of ids
func lessFuncPosBased(ids []seq.IDSource) func(a, b seq.IDSource) bool {
	positions := make(map[seq.IDSource]uint32, len(ids))
	for i, id := range ids {
		positions[seq.IDSource{ID: id.ID, Source: id.Source}] = uint32(i)
	}
	return func(a, b seq.IDSource) bool {
		aPos, aOk := positions[a]
		bPos, bOk := positions[b]
		if !aOk || !bOk {
			if !aOk && !bOk {
				panic("attempt to compare unknown IDSources")
			}
			// a is unknown, we treat it as true
			// b is unknown: we treat it as false
			// we treat the unknown as the smallest to pass it first and move on to the next document in the stream
			return !aOk
		}
		return aPos < bPos
	}
}

func (si *Ingestor) FetchDocsStream(ctx context.Context, ids []seq.IDSource, explain bool) (DocsIterator, error) {
	errs := make([]error, 0)
	streams := make([]DocsIterator, 0)
	for source, ids := range groupIDsBySource(ids) {
		if stream, err := si.singleDocsStream(ctx, explain, source, ids); err != nil {
			metric.FetchErrors.Inc()
			errs = append(errs, err)
			logger.Error("fetch error", zap.Error(err), zap.String("store", si.clientBySource[source]))
		} else {
			streams = append(streams, stream)
		}
	}

	if len(errs) > 0 && len(streams) == 0 {
		return nil, fmt.Errorf("all shards requests failed: %w", util.CollapseErrors(errs))
	}

	// if we have at least one stream, we continue processing
	return newMergedStreamIterator(ctx, streams, ids), nil
}

func expandIDsBySources(orig []seq.ID, clientBySource map[uint64]string) []seq.IDSource {
	ids := make([]seq.IDSource, 0, len(orig)*len(clientBySource))
	for _, id := range orig {
		for source := range clientBySource {
			ids = append(ids, seq.IDSource{ID: id, Source: source})
		}
	}
	return ids
}

func (si *Ingestor) Documents(ctx context.Context, ids []seq.ID) (DocsIterator, error) {
	metric.DocumentsRequested.Observe(float64(len(ids)))
	// todo: ***REMOVED*** we fetch from both stores hot and cold there; need fix that
	docsStream, err := si.FetchDocsStream(ctx, expandIDsBySources(ids, si.clientBySource), false)
	if err != nil {
		return nil, err
	}

	return newUniqueIDIterator(docsStream), nil
}

func (si *Ingestor) Document(ctx context.Context, id seq.ID) []byte {
	docsStream, err := si.Documents(ctx, []seq.ID{id})
	if err != nil {
		return nil
	}
	docs := ReadAll(docsStream)
	if len(docs) == 0 {
		logger.Warn("doc not found", zap.String("id", id.String()))
		return nil
	}
	if len(docs) > 1 {
		logger.Error("to many docs in result", zap.String("id", id.String()), zap.Any("docs", docs))
		return nil
	}
	return docs[0]
}

func (si *Ingestor) makeFetchReq(ids []seq.IDSource, explain bool) *storeapi.FetchRequest {
	idsWithHints := make([]*storeapi.IdWithHint, 0, len(ids))
	idsStr := make([]string, 0, len(ids))
	if len(ids) > 0 {
		lastID := ids[0]
		for _, id := range ids {
			if id.ID.MID > lastID.ID.MID {
				logger.Error("wrong fetch req, ids should be sorted",
					zap.String("prev", lastID.ID.String()),
					zap.String("cur", id.ID.String()),
				)
			}
			lastID = id
			idsStr = append(idsStr, id.ID.String())
			idsWithHints = append(idsWithHints, &storeapi.IdWithHint{
				Id:   id.ID.String(),
				Hint: id.Hint,
			})
		}
	}

	return &storeapi.FetchRequest{
		IdsWithHints: idsWithHints,
		Ids:          idsStr,
		Explain:      explain,
	}
}

// todo: consider from param
func (si *Ingestor) aggregate(times []time.Time, docs []int, to, interval seq.MID, ids []seq.ID) ([]time.Time, []int, seq.MID) {
	curTime := to
	curTime /= interval
	curTime *= interval // let's start with discrete point
	if len(docs) == 0 {
		docs = append(docs, 0)
		times = append(times, time.Unix(0, 0).Add(seq.MIDToDuration(curTime)))
	}
	for _, id := range ids {
		t := id.MID
		if t > curTime+interval {
			continue
		}

		for t < curTime {
			curTime -= interval
			times = append(times, time.Unix(0, 0).Add(seq.MIDToDuration(curTime)))
			docs = append(docs, 0)
		}
		docs[len(docs)-1]++
	}

	return times, docs, curTime
}

func (si *Ingestor) searchStores(ctx context.Context, sr *SearchRequest, s *stores.Stores) ([]*seq.QPR, error) {
	request := sr.GetAPISearchRequest()

	// this cancellation can be useful in the case where we received an ErrIngestorQueryWantsOldData error
	// from one store and we do not need to continue executing queries in the remaining stores
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type ShardResponse struct {
		Data   *storeapi.SearchResponse
		Source uint64
		Err    error
	}

	wg := sync.WaitGroup{}
	wg.Add(len(s.Shards))
	respChan := make(chan ShardResponse, len(s.Shards))
	for _, shard := range s.Shards {
		go func(shard []string) {
			defer wg.Done()

			resp, source, err := si.searchShard(ctx, shard, request)
			respChan <- ShardResponse{
				Data:   resp,
				Source: source,
				Err:    err,
			}
		}(shard)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	qprs := make([]*seq.QPR, 0, len(s.Shards))
	var errs []error
	for resp := range respChan {
		if err := resp.Err; err != nil {
			if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
				// At least one hot store doesn't have such old data
				return nil, err
			}
			errs = append(errs, err)
			continue
		}

		qprs = append(qprs, responseToQPR(resp.Data, resp.Source, sr.Explain))
	}

	if err := util.DeduplicateErrors(errs); err != nil {
		if len(qprs) != 0 {
			// There are errors, but some Shards returned data, so provide it to user
			return qprs, fmt.Errorf("%w: %s", consts.ErrPartialResponse, err)
		}
		return nil, err
	}

	return qprs, nil
}

func responseToQPR(resp *storeapi.SearchResponse, source uint64, explain bool) *seq.QPR {
	if explain {
		logger.Info("received from seq-db",
			zap.Int("len_ids", len(resp.IdSources)),
			zap.Uint64("found", resp.Total),
		)
	}

	ids := make(seq.IDSources, 0, len(resp.IdSources))
	for _, idSource := range resp.IdSources {
		ids = append(ids, seq.IDSource{
			ID: seq.ID{
				MID: seq.MID(idSource.Id.Mid),
				RID: seq.RID(idSource.Id.Rid),
			},
			Source: source,
			Hint:   idSource.Hint,
		})
	}

	hist := make(map[seq.MID]uint64, len(resp.Histogram))
	for k, v := range resp.Histogram {
		hist[seq.MID(k)] = v
	}

	aggs := make([]seq.QPRHistogram, len(resp.Aggs))
	for i, agg := range resp.Aggs {
		from := agg.AggHistogram
		to := make(map[string]*seq.AggregationHistogram)
		for k, v := range from {
			to[k] = &seq.AggregationHistogram{
				Min:       v.Min,
				Max:       v.Max,
				Sum:       v.Sum,
				Total:     v.Total,
				Samples:   v.Samples,
				NotExists: v.NotExists,
			}
		}
		aggs[i] = seq.QPRHistogram{
			HistogramByToken: to,
			NotExists:        agg.NotExists,
		}
	}

	errSources := make([]seq.ErrorSource, 0, len(resp.Errors))
	for _, err := range resp.Errors {
		errSources = append(errSources, seq.ErrorSource{
			ErrStr: err,
			Source: source,
		})
	}

	return &seq.QPR{
		IDs:       ids,
		Histogram: hist,
		Aggs:      aggs,
		Total:     resp.Total,
		Errors:    errSources,
	}
}

// searchShard searches on all hosts of given shard. Returns err only if all hosts return err.
func (si *Ingestor) searchShard(ctx context.Context, hosts []string, request *storeapi.SearchRequest) (*storeapi.SearchResponse, uint64, error) {
	var idx []int
	if si.config.ShuffleReplicas {
		idx = util.IdxShuffle(len(hosts))
	} else {
		idx = util.IdxFill(len(hosts))
	}

	var errs []error
	for i := 0; i < len(hosts); i++ {
		resp, source, err := si.searchHost(ctx, request, hosts[idx[i]])

		// TODO: remove in future versions
		if err != nil {
			errMessage := status.Convert(err).Message()
			if errMessage == consts.ErrIngestorQueryWantsOldData.Error() {
				// only hot store can return such error
				// fail fast in this case
				return nil, source, fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData)
			}
			if errMessage == consts.ErrTooManyUniqValues.Error() {
				return nil, source, fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyUniqValues)
			}
			errs = append(errs, err)
			continue
		}

		switch resp.Code {
		case storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA:
			return nil, source, fmt.Errorf("hot store refuses: %w", consts.ErrIngestorQueryWantsOldData)
		case storeapi.SearchErrorCode_TOO_MANY_UNIQ_VALUES:
			return nil, source, fmt.Errorf("store forbids aggregation request: %w", consts.ErrTooManyUniqValues)
		case storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT:
			return nil, source, fmt.Errorf("store forbids request: %w", consts.ErrTooManyFractionsHit)
		}

		return resp, source, nil
	}

	return nil, 0, util.DeduplicateErrors(errs)
}

// searchHost search req on single host.
func (si *Ingestor) searchHost(ctx context.Context, req *storeapi.SearchRequest, host string) (*storeapi.SearchResponse, uint64, error) {
	client, has := si.clients[host]
	if !has {
		logger.Panic("internal connection map corruption, no client for host",
			zap.String("host", host),
		)
	}

	data, err := client.Search(ctx, req,
		grpc.MaxCallRecvMsgSize(256*consts.MB),
		grpc.MaxCallSendMsgSize(256*consts.MB),
		grpc.UseCompressor(gzip.Name),
	)
	if err != nil {
		return nil, 0, err
	}

	return data, si.sourceByClient[host], nil
}

type IngestorStatus struct {
	NumberOfStores    int
	OldestStorageTime *time.Time
	Stores            []StoreStatus
}

type StoreStatus struct {
	Host       string
	OldestTime time.Time
	Error      string
}

func (si *Ingestor) Status(ctx context.Context) *IngestorStatus {
	storesHosts := si.getStoresHosts()

	wg := sync.WaitGroup{}
	wg.Add(len(storesHosts))
	respChan := make(chan StoreStatus, len(storesHosts))

	for _, host := range storesHosts {
		go func(host string) {
			defer wg.Done()

			client, has := si.clients[host]
			if !has {
				respChan <- StoreStatus{Host: host, Error: fmt.Sprintf("no client for host: %s", host)}
				return
			}
			resp, err := client.Status(ctx, &storeapi.StatusRequest{})
			if err != nil {
				respChan <- StoreStatus{Host: host, Error: err.Error()}
				return
			}
			respChan <- StoreStatus{Host: host, OldestTime: resp.OldestTime.AsTime()}
		}(host)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	var oldestTime *time.Time

	storesStatuses := make([]StoreStatus, 0, len(storesHosts))
	for resp := range respChan {
		if resp.Error != "" {
			storesStatuses = append(storesStatuses, StoreStatus{
				Host:  resp.Host,
				Error: resp.Error,
			})
			continue
		}
		ot := resp.OldestTime
		storesStatuses = append(storesStatuses, StoreStatus{
			Host:       resp.Host,
			OldestTime: ot,
		})
		if oldestTime == nil || ot.Before(*oldestTime) {
			oldestTime = &ot
		}
	}

	return &IngestorStatus{
		NumberOfStores:    len(storesHosts),
		OldestStorageTime: oldestTime,
		Stores:            storesStatuses,
	}
}

func (si *Ingestor) getStoresHosts() []string {
	numberOfStores := len(si.config.HotStores.Shards) + len(si.config.HotReadStores.Shards) +
		len(si.config.ReadStores.Shards) + len(si.config.WriteStores.Shards)

	hosts := make([]string, 0, numberOfStores)
	seen := make(map[string]struct{}, numberOfStores)

	shards := make([][]string, 0, numberOfStores)
	shards = append(shards, si.config.HotStores.Shards...)
	shards = append(shards, si.config.HotReadStores.Shards...)
	shards = append(shards, si.config.ReadStores.Shards...)
	shards = append(shards, si.config.WriteStores.Shards...)

	for _, shard := range shards {
		for _, host := range shard {
			if _, ok := seen[host]; ok {
				continue
			}
			hosts = append(hosts, host)
			seen[host] = struct{}{}
		}
	}

	return hosts
}
