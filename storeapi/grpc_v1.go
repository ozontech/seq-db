package storeapi

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type SearchConfig struct {
	WorkersCount          int
	MaxFractionHits       int
	FractionsPerIteration int
	RequestsLimit         uint64
	LogThreshold          time.Duration
	Async                 fracmanager.AsyncSearcherConfig
}

type BulkConfig struct {
	RequestsLimit uint64
	LogThreshold  time.Duration
}

type FetchConfig struct {
	LogThreshold time.Duration
}

type APIConfig struct {
	StoreMode string
	Bulk      BulkConfig
	Search    SearchConfig
	Fetch     FetchConfig
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

type bulkData struct {
	appendQueue atomic.Uint64
	took        atomic.Uint64
	batches     atomic.Uint64
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

	bulkData      bulkData
	searchData    searchData
	fetchData     fetchData
	asyncSearcher *fracmanager.AsyncSearcher

	inflightBulks atomic.Int64
}

func NewGrpcV1(config APIConfig, fracManager *fracmanager.FracManager, mappingProvider MappingProvider) *GrpcV1 {
	as := fracmanager.MustStartAsync(config.Search.Async, mappingProvider, fracManager.GetAllFracs())
	g := &GrpcV1{
		config:          config,
		fracManager:     fracManager,
		mappingProvider: mappingProvider,
		searchData: searchData{
			searcher: fracmanager.NewSearcher(config.Search.WorkersCount, fracmanager.SearcherCfg{
				MaxFractionHits:       config.Search.MaxFractionHits,
				FractionsPerIteration: config.Search.FractionsPerIteration,
			}),
		},
		fetchData: fetchData{
			docFetcher: fracmanager.NewFetcher(conf.FetchWorkers),
		},
		asyncSearcher: as,
	}

	go g.bulkStats()

	return g
}

func (g *GrpcV1) bulkStats() {
	for {
		stats := g.fracManager.GetActiveFrac().Info()
		if stats.Name() == "" {
			time.Sleep(time.Second * 5)
			continue
		}
		docs := stats.DocsTotal
		size := stats.DocsRaw
		fracName := stats.Name()
		time.Sleep(time.Second * 5)
		if g.bulkData.batches.Load() > 0 {
			stats = g.fracManager.GetActiveFrac().Info()
			if fracName != stats.Name() {
				continue
			}
			docs = stats.DocsTotal - docs
			size = stats.DocsRaw - size
			tookMs := util.DurationToUnit(time.Duration(g.bulkData.took.Load()), "ms")
			tookPerBatch := tookMs / float64(g.bulkData.batches.Load())
			tookPerDoc := tookMs / float64(docs)
			tookPerSize := tookMs / util.SizeToUnit(size, "mb")
			logger.Info("bulk api stats for 5s",
				zap.Uint64("batches", g.bulkData.batches.Load()),
				zap.Uint32("docs", docs),
				util.ZapUint64AsSizeStr("size", size),
				util.ZapFloat64WithPrec("took_per_batch_ms", tookPerBatch, 1),
				util.ZapFloat64WithPrec("took_per_doc_ms", tookPerDoc, 4),
				util.ZapFloat64WithPrec("took_per_size_ms", tookPerSize, 1),
				zap.Uint64("append_queue", g.bulkData.appendQueue.Load()),
			)
			g.bulkData.batches.Store(0)
			g.bulkData.took.Store(0)
		} else {
			logger.Info("bulk api stats for 5s: no batches have been written")
		}
	}
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
	if errors.Is(e, consts.ErrTooManyUniqValues) {
		return storeapi.SearchErrorCode_TOO_MANY_UNIQ_VALUES, true
	}

	if errors.Is(e, consts.ErrTooManyFractionsHit) {
		metric.RejectedRequests.WithLabelValues("search", "fracs_exceeding").Inc()
		return storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT, true
	}

	return 0, false
}
