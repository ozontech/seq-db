package storeapi

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/sortkeys"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/search"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func (g *GrpcV1) Search(ctx context.Context, req *storeapi.SearchRequest) (*storeapi.SearchResponse, error) {
	ctx, span := tracing.StartSpan(ctx, "store-server/Search")
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("request", req.Query))
		span.AddAttributes(trace.Int64Attribute("from", req.From))
		span.AddAttributes(trace.Int64Attribute("to", req.From))
		span.AddAttributes(trace.Int64Attribute("size", req.Size))
		span.AddAttributes(trace.Int64Attribute("offset", req.Offset))
		span.AddAttributes(trace.Int64Attribute("interval", req.Interval))
		span.AddAttributes(trace.BoolAttribute("explain", req.Explain))
		span.AddAttributes(trace.BoolAttribute("with_total", req.WithTotal))
		span.AddAttributes(trace.StringAttribute("aggregation_filter", req.AggregationFilter))
	}

	data, err := g.doSearch(ctx, req)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		logger.Error("search error", zap.Error(err), zap.Object("request", (*searchRequestMarshaler)(req)))
	}
	return data, err
}

var aggAsteriskFilter = "*"

func (g *GrpcV1) doSearch(ctx context.Context, req *storeapi.SearchRequest) (*storeapi.SearchResponse, error) {
	metric.SearchInFlightQueriesTotal.Inc()
	defer metric.SearchInFlightQueriesTotal.Dec()

	inflightRequests := g.searchData.inflight.Inc()
	defer g.searchData.inflight.Dec()

	if inflightRequests > int64(g.config.Search.RequestsLimit) {
		metric.RejectedRequests.WithLabelValues("search", "limit_exceeding").Inc()
		return nil, fmt.Errorf("too many search requests: %d > %d", inflightRequests, g.config.Search.RequestsLimit)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	from := seq.MID(req.From)

	// in store mode hot we return error in case request wants data, that we've already rotated
	if g.config.StoreMode == StoreModeHot {
		if g.fracManager.Mature() && g.earlierThanOldestFrac(uint64(from)) {
			metric.RejectedRequests.WithLabelValues("search", "old_data").Inc()
			return &storeapi.SearchResponse{Code: storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA}, nil
		}
	}

	to := seq.MID(req.To)
	limit := int(req.Size + req.Offset)

	searchCell := frac.NewSearchCell(ctx)
	searchCell.Explain = req.Explain

	if searchCell.Explain {
		logger.Info("search request will be explained",
			zap.Any("request", req),
		)
	}

	t := time.Now()
	if req.Query == "" {
		req.Query = seq.TokenAll + ":*"
	}
	ast, err := parser.ParseQuery(req.Query, g.mapping)
	searchCell.AddParseTime(time.Since(t))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "can't parse query %q: %v", req.Query, err)
	}

	if searchCell.IsCancelled() {
		return nil, fmt.Errorf("search cancelled before evaluating: reason=%w", searchCell.Context.Err())
	}

	aggQ := make([]search.AggQuery, 0, len(req.Aggs))
	for _, aggQuery := range req.Aggs {
		aggFunc, err := aggQueryFromProto(aggQuery)
		if err != nil {
			return nil, err
		}
		aggQ = append(aggQ, aggFunc)
	}

	const millisecondsInSecond = float64(time.Second / time.Millisecond)
	metric.SearchRangesSeconds.Observe(float64(to-from) / millisecondsInSecond)

	searchParams := search.Params{
		AST:  ast,
		AggQ: aggQ,
		AggLimits: search.AggLimits{
			MaxFieldTokens:     g.config.Search.Aggregation.MaxFieldTokens,
			MaxGroupTokens:     g.config.Search.Aggregation.MaxGroupTokens,
			MaxTIDsPerFraction: g.config.Search.Aggregation.MaxTIDsPerFraction,
		},
		HistInterval: uint64(req.Interval),
		From:         from,
		To:           to,
		Limit:        limit,
		WithTotal:    req.WithTotal,
		Order:        req.Order.MustDocsOrder(),
	}

	qpr, stats, evalDuration, err := g.searchIteratively(searchCell, searchParams, g.config.Search.FractionsPerIteration)
	if err != nil {
		if errors.Is(err, consts.ErrTooManyUniqValues) {
			return &storeapi.SearchResponse{Code: storeapi.SearchErrorCode_TOO_MANY_UNIQ_VALUES}, nil
		}

		return nil, err
	}

	updateSearchStatsMetrics(stats, evalDuration.Seconds())

	searchCell.AddOverallTime(time.Since(start))
	searchCell.AddFound(qpr.Total)

	updateSearchCellMetrics(searchCell)

	if searchCell.Explain {
		logger.Info(searchCell.ExplainSB.String())

		if req.Interval > 0 {
			keys := make(sortkeys.Uint64Slice, 0)
			for key := range qpr.Histogram {
				keys = append(keys, uint64(key))
			}
			sort.Sort(keys)
			for _, key := range keys {
				logger.Info("histogram",
					zap.Int64("t", t.UnixNano()),
					zap.String("q", req.Query),
					zap.Uint64("key", key),
					zap.Uint64("val", qpr.Histogram[seq.MID(key)]),
				)
			}
		}
	}

	took := time.Since(start)
	if g.config.Search.LogThreshold != 0 && took >= g.config.Search.LogThreshold {
		logger.Warn("slow search",
			zap.Int64("took_ms", took.Milliseconds()),
			zap.Object("req", (*searchRequestMarshaler)(req)),
			zap.Uint64("found", qpr.Total),
			zap.String("from", seq.MID(req.From).String()),
			zap.String("to", seq.MID(req.To).String()),
			zap.Int64("offset", req.Offset),
			zap.Int64("size", req.Size),
			zap.Bool("with_total", req.WithTotal),
		)
	}

	return buildSearchResponse(qpr, searchCell), nil
}

func (g *GrpcV1) searchIteratively(searchCell *frac.SearchCell, params search.Params, n int) (*seq.QPR, []*search.Stats, time.Duration, error) {
	remainingFracs, err := g.fracManager.SelectFracsInRange(params.From, params.To)
	if err != nil {
		return nil, nil, 0, err
	}

	if params.Order.IsReverse() {
		sort.Slice(remainingFracs, func(i, j int) bool { // ascending order by From
			return remainingFracs[i].Info().From < remainingFracs[j].Info().From
		})
	} else {
		sort.Slice(remainingFracs, func(i, j int) bool { // descending order by To
			return remainingFracs[i].Info().To > remainingFracs[j].Info().To
		})
	}

	origLimit := params.Limit
	scanAll := params.IsScanAllRequest()
	allStats := make([]*search.Stats, 0, len(remainingFracs))

	var (
		total = &seq.QPR{
			Histogram: make(map[seq.MID]uint64),
			Aggs:      make([]seq.QPRHistogram, len(params.AggQ)),
		}
		subQueriesCount float64
		totalSearchTime time.Duration
	)

	for len(remainingFracs) > 0 && (scanAll || params.Limit > 0) {
		var fracsToSearch fracmanager.FracsList
		fracsToSearch, remainingFracs = splitByCount(remainingFracs, n)

		t := time.Now()
		subQPRs, stats, err := g.searchData.workerPool.Search(searchCell.Context, fracsToSearch, params)
		searchTime := time.Since(t)
		searchCell.AddEvaluationTime(searchTime)

		totalSearchTime += searchTime
		if err != nil {
			return nil, allStats, totalSearchTime, err
		}

		t = time.Now()
		seq.MergeQPRs(total, subQPRs, origLimit, seq.MID(params.HistInterval), params.Order)
		searchCell.AddMergeTime(time.Since(t))

		// reduce the limit on the number of ensured docs in response
		params.Limit = origLimit - countIDsAfter(total.IDs, rightmostBorder(remainingFracs))
		allStats = append(allStats, stats...)
		subQueriesCount++
	}

	if total == nil {
		total = &seq.QPR{}
	}

	metric.SearchSubSearches.Observe(subQueriesCount)
	return total, allStats, totalSearchTime, nil
}

func (g *GrpcV1) earlierThanOldestFrac(from uint64) bool {
	oldestCt := g.fracManager.OldestCT.Load()
	return oldestCt == 0 || oldestCt > from
}

func buildSearchResponse(qpr *seq.QPR, searchCell *frac.SearchCell) *storeapi.SearchResponse {
	idSourcesBuf := make([]storeapi.SearchResponse_IdWithHint, len(qpr.IDs))
	idSources := make([]*storeapi.SearchResponse_IdWithHint, len(qpr.IDs))
	for i := range qpr.IDs {
		idSourcesBuf[i].Id = &storeapi.SearchResponse_Id{
			Mid: uint64(qpr.IDs[i].ID.MID),
			Rid: uint64(qpr.IDs[i].ID.RID),
		}
		idSourcesBuf[i].Hint = qpr.IDs[i].Hint

		idSources[i] = &idSourcesBuf[i]
	}

	// convert map[MID]uint64 -> map[uint64]uint64
	// because Go can not convert MID to uint64
	// and protobuf can not use our MID data type
	histogram := make(map[uint64]uint64, len(qpr.Histogram))
	for k, v := range qpr.Histogram {
		histogram[uint64(k)] = v
	}

	aggsBuf := make([]storeapi.SearchResponse_Agg, len(qpr.Aggs))
	aggs := make([]*storeapi.SearchResponse_Agg, len(qpr.Aggs))
	for i, fromAgg := range qpr.Aggs {
		from := fromAgg.HistogramByToken
		to := make(map[string]*storeapi.SearchResponse_Histogram, len(from))
		toAgg := make(map[string]uint64, len(from))
		for k, v := range from {
			to[k] = &storeapi.SearchResponse_Histogram{
				Min:       v.Min,
				Max:       v.Max,
				Sum:       v.Sum,
				Total:     v.Total,
				Samples:   v.Samples,
				NotExists: v.NotExists,
			}
			toAgg[k] = uint64(v.Total)
		}
		aggsBuf[i].NotExists = fromAgg.NotExists
		aggsBuf[i].AggHistogram = to
		aggsBuf[i].Agg = toAgg
		aggs[i] = &aggsBuf[i]
	}

	qprErrs := make([]string, len(qpr.Errors))
	for i, qprErr := range searchCell.Errors {
		qprErrs[i] = qprErr.Error()
	}

	return &storeapi.SearchResponse{
		IdSources: idSources,
		Histogram: histogram,
		Aggs:      aggs,
		Total:     qpr.Total,
		Errors:    qprErrs,
	}
}

func aggQueryFromProto(aggQuery *storeapi.AggQuery) (search.AggQuery, error) {
	// 'groupBy' is required for Count and Unique.
	if aggQuery.GroupBy == "" && (aggQuery.Func == storeapi.AggFunc_AGG_FUNC_COUNT || aggQuery.Func == storeapi.AggFunc_AGG_FUNC_UNIQUE) {
		return search.AggQuery{}, fmt.Errorf("%w: groupBy is required for %s func", consts.ErrInvalidAggQuery, aggQuery.Func)
	}
	// 'field' is required for stat functions like sum, avg, max and min.
	if aggQuery.Field == "" && aggQuery.Func != storeapi.AggFunc_AGG_FUNC_COUNT && aggQuery.Func != storeapi.AggFunc_AGG_FUNC_UNIQUE {
		return search.AggQuery{}, fmt.Errorf("%w: field is required for %s func", consts.ErrInvalidAggQuery, aggQuery.Func)
	}
	// Check 'quantiles' is not empty for Quantile func.
	if len(aggQuery.Quantiles) == 0 && aggQuery.Func == storeapi.AggFunc_AGG_FUNC_QUANTILE {
		return search.AggQuery{}, fmt.Errorf("%w: expect an argument for Quantile func", consts.ErrInvalidAggQuery)
	}

	var field *parser.Literal
	if aggQuery.Field != "" {
		field = &parser.Literal{
			Field: aggQuery.Field,
			Terms: searchAll,
		}
	}

	var groupBy *parser.Literal
	if aggQuery.GroupBy != "" {
		groupBy = &parser.Literal{
			Field: aggQuery.GroupBy,
			Terms: searchAll,
		}
	}

	aggFunc, err := aggQuery.Func.ToAggFunc()
	if err != nil {
		return search.AggQuery{}, err
	}

	return search.AggQuery{
		Field:     field,
		GroupBy:   groupBy,
		Func:      aggFunc,
		Quantiles: aggQuery.Quantiles,
	}, nil
}

var searchAll = []parser.Term{{
	Kind: parser.TermSymbol, Data: aggAsteriskFilter,
}}

func splitByCount(fl fracmanager.FracsList, n int) (fracmanager.FracsList, fracmanager.FracsList) {
	if n > len(fl) {
		n = len(fl)
	}
	return fl[:n], fl[n:]
}

func rightmostBorder(fl fracmanager.FracsList) seq.MID {
	if len(fl) == 0 {
		return 0
	}
	return fl[0].Info().To
}

func countIDsAfter(ids seq.IDSources, mid seq.MID) int {
	if mid == 0 {
		return len(ids)
	}
	return sort.Search(len(ids), func(i int) bool { return ids[i].ID.MID <= mid })
}

type aggQueryMarshaler storeapi.AggQuery

func (s *aggQueryMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("field", s.Field)
	enc.AddString("groupBy", s.GroupBy)
	enc.AddString("func", s.Func.String())
	enc.AddInt("quantiles_count", len(s.Quantiles))
	return nil
}

type aggQuerySliceMarshaler []*storeapi.AggQuery

func (s aggQuerySliceMarshaler) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, q := range s {
		_ = enc.AppendObject((*aggQueryMarshaler)(q))
	}
	return nil
}

type searchRequestMarshaler storeapi.SearchRequest

func (s *searchRequestMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("query", s.Query)
	enc.AddString("aggregation_filter", s.AggregationFilter)
	enc.AddString("aggregation", s.Aggregation)
	_ = enc.AddArray("aggs", aggQuerySliceMarshaler(s.Aggs))

	enc.AddString("from", util.MsTsToESFormat(uint64(s.From)))
	enc.AddString("to", util.MsTsToESFormat(uint64(s.To)))

	enc.AddInt64("size", s.Size)
	enc.AddInt64("offset", s.Offset)
	enc.AddInt64("interval", s.Interval)
	enc.AddBool("explain", s.Explain)
	enc.AddBool("with_total", s.WithTotal)

	return nil
}

func updateSearchStatsMetrics(stats []*search.Stats, evalDuration float64) {
	for _, stat := range stats {
		metric.SearchLeavesTotal.Observe(float64(stat.LeavesTotal))
		metric.SearchNodesTotal.Observe(float64(stat.NodesTotal))
		metric.SearchSourcesTotal.Observe(float64(stat.SourcesTotal))
		metric.SearchAggNodesTotal.Observe(float64(stat.AggNodesTotal))
		metric.SearchHitsTotal.Observe(float64(stat.HitsTotal))
	}
	metric.SearchEvalDurationSeconds.Observe(evalDuration)
}

func updateSearchCellMetrics(sc *frac.SearchCell) {
	metric.SearchDurationSeconds.Observe(float64(sc.ExplainSB.OverallTimeNS.Load()) / float64(time.Second))

	metric.ReadIDTimeNSTotal.Add(float64(sc.ExplainSB.ReadIDTimeNS.Load()))
	metric.ReadLIDTimeNSTotal.Add(float64(sc.ExplainSB.ReadLIDTimeNS.Load()))
	metric.ReadFieldTimeNSTotal.Add(float64(sc.ExplainSB.ReadFieldTimeNS.Load()))

	metric.DecodeIDTimeNSTotal.Add(float64(sc.ExplainSB.DecodeIDTimeNS.Load()))
	metric.DecodeLIDTimeNSTotal.Add(float64(sc.ExplainSB.DecodeLIDTimeNS.Load()))
	metric.DecodeTIDTimeNSTotal.Add(float64(sc.ExplainSB.DecodeTIDTimeNS.Load()))
	metric.DecodeFieldTimeNSTotal.Add(float64(sc.ExplainSB.DecodeFieldTimeNS.Load()))
}
