package storeapi

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ozontech/seq-db/asyncsearcher"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type SearchConfig struct {
	WorkersCount          int
	MaxFractionHits       int
	FractionsPerIteration int
	MaxQprMemory          int // max heap memory a single QPR can use (bytes). 0 if no limit set.
	RequestsLimit         uint64
	LogThreshold          time.Duration
	Async                 asyncsearcher.AsyncSearcherConfig
}

type BulkConfig struct {
	RequestsLimit uint64
	LogThreshold  time.Duration
}

type FetchConfig struct {
	LogThreshold time.Duration
}

type FilterConfig struct {
	Query string    `config:"query"`
	From  time.Time `config:"from"`
	To    time.Time `config:"to"`
}

type APIConfig struct {
	StoreMode string
	Bulk      BulkConfig
	Search    SearchConfig
	Fetch     FetchConfig
	Filter    FilterConfig
}

func (c *APIConfig) setDefaults() error {
	if c.Bulk.RequestsLimit == 0 {
		c.Bulk.RequestsLimit = consts.DefaultBulkRequestsLimit
		logger.Warn("wrong StoreConfig.BulkRequestsLimit value (0) is fixed", zap.Uint64("new_value", c.Bulk.RequestsLimit))
	}
	if c.Search.RequestsLimit == 0 {
		c.Search.RequestsLimit = consts.DefaultSearchRequestsLimit
		logger.Warn("wrong StoreConfig.SearchRequestsLimit value (0) is fixed", zap.Uint64("new_value", c.Search.RequestsLimit))
	}
	if c.Search.FractionsPerIteration == 0 {
		return fmt.Errorf("invalid StoreConfig.FractionsPerSearchIteration param in config")
	}
	return nil
}

type searchData struct {
	searcher *fracmanager.Searcher
	inflight atomic.Int64
}

type fetchData struct {
	docFetcher *fracmanager.Fetcher
}

type GrpcV1 struct {
	storeapi.UnimplementedStoreApiServer
	blank emptypb.Empty

	config APIConfig

	fracManager     *fracmanager.FracManager
	mappingProvider MappingProvider

	searchData    searchData
	fetchData     fetchData
	asyncSearcher *asyncsearcher.AsyncSearcher

	inflightBulks atomic.Int64
}

func NewGrpcV1(cfg APIConfig, fracManager *fracmanager.FracManager, mappingProvider MappingProvider) *GrpcV1 {
	g := &GrpcV1{
		config:          cfg,
		fracManager:     fracManager,
		mappingProvider: mappingProvider,
		searchData: searchData{
			searcher: fracmanager.NewSearcher(cfg.Search.WorkersCount, fracmanager.SearcherCfg{
				MaxFractionHits:       cfg.Search.MaxFractionHits,
				FractionsPerIteration: cfg.Search.FractionsPerIteration,
				MaxQprMemory:          cfg.Search.MaxQprMemory,
			}),
		},
		fetchData: fetchData{
			docFetcher: fracmanager.NewFetcher(config.FetchWorkers),
		},
		asyncSearcher: asyncsearcher.MustStartAsync(
			cfg.Search.Async, mappingProvider,
			fracManager.Fractions(),
		),
	}

	return g
}

func tracerSpanToExplainEntry(span *querytracer.Span) *storeapi.ExplainEntry {
	if span == nil {
		return nil
	}

	ee := &storeapi.ExplainEntry{
		Message:  span.Message,
		Duration: durationpb.New(span.Duration),
	}

	for _, child := range span.Children {
		ee.Children = append(ee.Children, tracerSpanToExplainEntry(child))
	}

	return ee
}

func parseStoreError(e error) (storeapi.SearchErrorCode, bool) {
	if errors.Is(e, consts.ErrTooManyFieldTokens) {
		return storeapi.SearchErrorCode_TOO_MANY_FIELD_TOKENS, true
	}
	if errors.Is(e, consts.ErrTooManyFieldValues) {
		return storeapi.SearchErrorCode_TOO_MANY_FIELD_VALUES, true
	}
	if errors.Is(e, consts.ErrTooManyGroupTokens) {
		return storeapi.SearchErrorCode_TOO_MANY_GROUP_TOKENS, true
	}
	if errors.Is(e, consts.ErrTooManyFractionTokens) {
		return storeapi.SearchErrorCode_TOO_MANY_FRACTION_TOKENS, true
	}
	if errors.Is(e, consts.ErrMemoryLimitExceeded) {
		return storeapi.SearchErrorCode_MEMORY_LIMIT_EXCEEDED, true
	}
	if errors.Is(e, consts.ErrTooManyFractionsHit) {
		metric.RejectedRequests.WithLabelValues("search", "fracs_exceeding").Inc()
		return storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT, true
	}

	return 0, false
}
