package proxyapi

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/proxy/search"
)

// requestObservation collects timing and storage-tier data for a single search
// request. The proxy fills it in doSearch and finalizes it (log + metric) with
// finish() from the calling handler, after the document stream has been read —
// so the recorded duration includes stream consumption.
type requestObservation struct {
	start         time.Time
	fetchStart    time.Time
	fetchDuration time.Duration
	stats         *search.SearchStats
	rawErr        error // raw ingestor error before status wrapping (preserves timeout type)
}

// search result categories used as the "result" metric label.
const (
	searchResultSuccess   = "success"
	searchResultClientErr = "client_error"
	searchResultServerErr = "server_error"
	searchResultTimeout   = "timeout"
)

func (o *requestObservation) takeFetchDuration() {
	if o.fetchDuration == 0 && !o.fetchStart.IsZero() {
		o.fetchDuration = time.Since(o.fetchStart)
	}
}

func (o *requestObservation) finish(method string, retErr error) {
	if o == nil {
		return
	}

	o.takeFetchDuration() // if we stopped fetching before reaching EOF, calc fetch duration here

	result, tier := classifySearchResult(retErr, o.rawErr, o.stats)
	fields := []zap.Field{
		zap.String("method", method),
		zap.Bool("agg", o.stats.HasAgg),
		zap.Bool("hist", o.stats.HasHist),
		zap.Int("docs", o.stats.Size),
		zap.String("tier", tier),
		zap.Duration("hot_duration", o.stats.HotSearchDuration),
		zap.Duration("total_search_duration", o.stats.TotalSearchDuration),
		zap.Duration("fetch_duration", o.fetchDuration),
		zap.Duration("total_duration", time.Since(o.start)),
		zap.String("result", result),
	}
	if tier == string(search.StorageTierCold) {
		fields = append(fields, zap.Duration("cold_duration", o.stats.ColdSearchDuration))
	}
	if retErr != nil {
		fields = append(fields, zap.NamedError("error", retErr))
	}
	logger.Info("search request stat", fields...)
	metric.SearchResults.WithLabelValues(result, tier).Inc()
}

// classifySearchResult maps the final handler error and the raw ingestor error
// to a result category and a storage tier. The raw error is used to detect
// timeouts, since processSearchErrors wraps them into codes.Internal.
func classifySearchResult(retErr, rawErr error, stats *search.SearchStats) (result, tier string) {
	tier = string(stats.StorageTier)
	if tier == "" {
		tier = string(search.StorageTierNone)
	}
	switch {
	case retErr == nil:
		return searchResultSuccess, tier
	case isTimeoutErr(rawErr), isTimeoutErr(retErr):
		return searchResultTimeout, tier
	case isClientErr(retErr, rawErr):
		return searchResultClientErr, tier
	default:
		return searchResultServerErr, tier
	}
}

func isTimeoutErr(e error) bool {
	if e == nil {
		return false
	}
	if errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled) {
		return true
	}
	return status.Code(e) == codes.DeadlineExceeded
}

func isClientErr(retErr, rawErr error) bool {
	if errors.Is(rawErr, consts.ErrInvalidArgument) {
		return true
	}
	switch status.Code(retErr) {
	case codes.InvalidArgument, codes.ResourceExhausted, codes.Canceled:
		return true
	}
	return false
}
