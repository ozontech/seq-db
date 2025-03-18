package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/encoding"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/network/circuitbreaker"
	"github.com/ozontech/seq-db/network/debugserver"
	"github.com/ozontech/seq-db/network/grpcutil"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/proxyapi"
	"github.com/ozontech/seq-db/storeapi"
	"github.com/ozontech/seq-db/tracing"
)

var (
	mode                   = kingpin.Flag("mode", `operation mode`).Default(appModeIngestor).HintOptions(appModeIngestor, appModeStore).String()
	addr                   = kingpin.Flag("addr", `listen addr e.g. ":9002"`).Default(":9002").String()
	proxyGrpcAddr          = kingpin.Flag("proxy-grpc-addr", `listen addr for grpc e.g. ":9004"`).Default(":9004").String()
	debugAddr              = kingpin.Flag("debug-addr", `debug listen addr e.g. ":9200"`).Default(":9200").String()
	readStores             = kingpin.Flag("read-stores", `list of store hosts to read`).String()
	writeStores            = kingpin.Flag("write-stores", `list of store hosts to write`).String()
	hotStores              = kingpin.Flag("hot-stores", `list of hot store hosts`).String()
	hotReadStores          = kingpin.Flag("hot-read-stores", `list of hot stores to read`).String()
	replicas               = kingpin.Flag("replicas", `replication factor for stores`).Default("1").Int()
	hotReplicas            = kingpin.Flag("hot-replicas", `replication factor for hot stores (if not set, global is used)`).Int()
	dataDir                = kingpin.Flag("data-dir", `directory to load/store data`).ExistingDir()
	fracSize               = kingpin.Flag("frac-size", `size of one fraction`).Default("128MB").Bytes()
	totalSize              = kingpin.Flag("total-size", `max size of all data`).Default("1GB").Bytes()
	cacheSize              = kingpin.Flag("cache-size", `max size of the cache`).Default("8GB").Bytes()
	mappingPath            = kingpin.Flag("mapping", `path to mapping file or 'auto' to index all fields`).Required().String()
	storeMode              = kingpin.Flag("store-mode", `store operation mode`).Default("").HintOptions("", storeapi.StoreModeCold, storeapi.StoreModeHot).String()
	queryRateLimit         = kingpin.Flag("query-rate-limit", `max requests per second`).Default("2.0").Float()
	logSearchThresholdMs   = kingpin.Flag("log-search-threshold-ms", `threshold for logging search queries, ms`).Default("3000").Int()
	logBulkThresholdMs     = kingpin.Flag("log-bulk-threshold-ms", `threshold for logging bulk requests, ms`).Int()
	logFetchThresholdMs    = kingpin.Flag("log-fetch-threshold-ms", `threshold for logging fetch requests, ms`).Default("3000").Int()
	bulkRequestsLimit      = kingpin.Flag("requests-limit", `maximum number of simultaneous bulk requests`).Default(strconv.Itoa(consts.DefaultBulkRequestsLimit)).Uint64()
	searchRequestsLimit    = kingpin.Flag("search-requests-limit", `maximum number of simultaneous search requests`).Default(strconv.Itoa(consts.DefaultSearchRequestsLimit)).Uint64()
	maxFractionHits        = kingpin.Flag("search-fraction-limit", `the maximum number of fractions used in the search`).Default(strconv.Itoa(consts.DefaultMaxFractionHits)).Int()
	allowedTimeDrift       = kingpin.Flag("allowed-time-drift", `maximum allowed time since the message timestamp`).Default(consts.AllowedTimeDrift).Duration()
	futureAllowedTimeDrift = kingpin.Flag("future-allowed-time-drift", `maximum future allowed time since the message timestamp`).Default(consts.FutureAllowedTimeDrift).Duration()
	maxInflightBulks       = kingpin.Flag("max-inflight-bulks", `max ingestor inflight bulk requests`).Default(strconv.Itoa(consts.IngestorMaxInflightBulks)).Int()
	shuffleReplicas        = kingpin.Flag("shuffle-replicas", `shuffle replicas before performing search`).Default("false").Bool()
	// Deprecated: do not use.
	_ = kingpin.Flag("max-pool-size", ``).Default("1024").Hidden().Int()
	// Deprecated: do not use.
	_                    = kingpin.Flag("max-buffer-cap", ``).Default("32768").Hidden().Int()
	maxTokenSize         = kingpin.Flag("max-token-size", ``).Default("72").Int()
	tracingProbability   = kingpin.Flag("tracing-probability", ``).Default("0.01").Float64()
	searchWorkersCount   = kingpin.Flag("search-workers-count", `the number of workers that will be process factions`).Default("128").Int()
	bulkShardTimeout     = kingpin.Flag("bulk-shard-timeout", `timeout for a shard to process bulk`).Default("10s").Duration()
	bulkErrPercentage    = kingpin.Flag("bulk-err-percentage", "check circuitbreaker/README.md for more details").Default("50").Int64()
	bulkBucketWidth      = kingpin.Flag("bulk-bucket-width", "check circuitbreaker/README.md for more details").Default("1s").Duration()
	bulkBucketsCount     = kingpin.Flag("bulk-err-count", "check circuitbreaker/README.md for more details").Default("10").Int()
	bulkSleepWindow      = kingpin.Flag("bulk-sleep-window", "check circuitbreaker/README.md for more details").Default("5s").Duration()
	bulkVolumeThreshold  = kingpin.Flag("bulk-request-volume-threshold", "check circuitbreaker/README.md for more details").Default("5").Int64()
	readerWorkers        = kingpin.Flag("reader-workers", "size of readers pool").Default(strconv.Itoa(consts.ReaderWorkers)).Uint64()
	caseSensitive        = kingpin.Flag("case-sensitive", "case insensitive to tokens").Default("false").Bool()
	skipFsync            = kingpin.Flag("skip-fsync", "skip fsyncs for active fraction").Default("false").Bool()
	maxSearchDocs        = kingpin.Flag("max-search-docs", `maximum number of documents returned by search query`).Default(strconv.Itoa(consts.DefaultMaxRequestedDocuments)).Uint64()
	partialFieldIndexing = kingpin.Flag("partial-indexing", `index only the first part of long fields`).Default("false").Bool()
	esVersion            = kingpin.Flag("es-version", "ES version to return in the `/` handler").Default("8.9.0").String()

	aggMaxGroupTokens     = kingpin.Flag("agg-max-group-tokens", `the maximum group tokens per aggregation, set 0 to disable limit`).Default(strconv.Itoa(consts.DefaultMaxGroupsPerAggregation)).Int()
	aggMaxFieldTokens     = kingpin.Flag("agg-max-field-tokens", `the maximum field tokens per aggregation, set 0 to disable limit`).Default(strconv.Itoa(consts.DefaultMaxFieldTokensPerAggregation)).Int()
	aggMaxTIDsPerFraction = kingpin.Flag("agg-max-fraction-tids", `the number of max unique tokens per fraction, set 0 to disable limit`).Default(strconv.Itoa(consts.DefaultMaxAggregatingTIDsPerFraction)).Int()

	mirrorAddr = kingpin.Flag("mirror-addr", `the address of the seqproxy mirror`).Default("").String()

	docsZSTDCompressLevel  = kingpin.Flag("docs-zstd-compress-level", `ZSTD compress level for docs, check the doc for more details: https://facebook.github.io/zstd/zstd_manual.html`).Default("3").Int()
	metasZSTDCompressLevel = kingpin.Flag("metas-zstd-compress-level", `ZSTD compress level for metas, check the doc for more details: https://facebook.github.io/zstd/zstd_manual.html`).Default("3").Int()
	sealCompressLevel      = kingpin.Flag("seal-zstd-compress-level", "ZSTD compress level that will be used to seal the active fraction: https://facebook.github.io/zstd/zstd_manual.html").Default("3").Int()

	maxDocSize = kingpin.Flag("max-document-size", "the maximum document size, documents larger than this will be skipped").Default("128KiB").Bytes()

	enableMappingUpdates = kingpin.Flag("enable-mapping-updates", "this will periodically check mapping file and reload configuration if there is an update").Default("false").Bool()
	mappingUpdatePeriod  = kingpin.Flag("mapping-update-period", "the amount of time to pass for the mappings to be reloaded").Default("30s").Duration()

	useSeqQLByDefault = kingpin.Flag("use-seq-ql-by-default", "enable seq-ql as default query language").Default("false").Bool()
)

const (
	appModeIngestor = "ingestor"
	appModeStore    = "store"
	appModeSingle   = "single"
)

func validateIngestorTopology(hotStores, hotReadStores, _, _ *stores.Stores) error {
	// TODO: on evidently wrong topology fail fast

	if len(hotStores.Shards) == 0 && len(hotReadStores.Shards) == 0 {
		logger.Warn("no hot stores are set")
	}

	return nil
}

func main() {
	encoding.RegisterCodec(grpcutil.VTProtoCodec{})
	rand.Seed(time.Now().UnixNano())

	logger.Info("hi, I am seq-db",
		zap.String("version", buildinfo.Version),
		zap.String("build_time", buildinfo.BuildTime),
	)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.Setenv("TZ", "UTC"); err != nil {
		logger.Fatal("can't set timezone to UTC", zap.Error(err))
	}

	kingpin.Parse()
	kingpin.Version(buildinfo.Version)

	runtime.SetMutexProfileFraction(5)
	_, _ = maxprocs.Set(maxprocs.Logger(func(tpl string, args ...any) {
		logger.Info(fmt.Sprintf(tpl, args...))
	}))
	numCPU := runtime.GOMAXPROCS(0)

	conf.IndexWorkers = numCPU
	conf.ReaderWorkers = int(*readerWorkers)
	conf.CaseSensitive = *caseSensitive
	conf.SkipFsync = *skipFsync
	conf.MaxRequestedDocuments = int(*maxSearchDocs)
	conf.UseSeqQLByDefault = *useSeqQLByDefault
	backoff.DefaultConfig.MaxDelay = 10 * time.Second

	var serviceReady atomic.Bool
	debugServer := debugserver.New(*debugAddr, &serviceReady)
	go debugServer.Start()

	var (
		store    *storeapi.Store
		ingestor *proxyapi.Ingestor
	)

	logger.Info("max queries per second", zap.Float64("limit", *queryRateLimit))

	if err := tracing.Start(*tracingProbability); err != nil {
		logger.Error("error initializing tracing", zap.Error(err))
	}

	mappingProvider, err := mappingprovider.New(
		*mappingPath,
		mappingprovider.WithUpdatePeriod(*mappingUpdatePeriod),
		mappingprovider.WithIndexAllFields(enableIndexingForAllFields(*mappingPath)),
	)
	if err != nil {
		logger.Fatal("load mapping error", zap.Error(err))
	}

	if *enableMappingUpdates {
		mappingProvider.WatchUpdates(ctx)
	}

	switch *mode {
	case appModeStore:
		store = startStore(ctx, *addr, mappingProvider)
	case appModeIngestor:
		ingestor = startProxy(ctx, *addr, mappingProvider, *caseSensitive, nil)
	case appModeSingle:
		store = startStore(ctx, "", mappingProvider)
		ingestor = startProxy(ctx, *addr, mappingProvider, *caseSensitive, store)
	default:
		logger.Fatal("unknown mode", zap.String("mode", *mode))
	}

	serviceReady.Store(true)

	<-ctx.Done()
	logger.Info("got signal to quit")

	// TODO: send root context ingestor
	if store != nil {
		store.Stop()
	}
	if ingestor != nil {
		ingestor.Stop()
	}

	debugServer.Stop(200 * time.Millisecond)

	logger.Info("quit")
}

func startProxy(_ context.Context, addr string, mp bulk.MappingProvider, caseSensitive bool, inMemory *storeapi.Store) *proxyapi.Ingestor {
	logger.Info("max queries per second", zap.Float64("limit", *queryRateLimit))

	hotReplicasNum := *replicas
	if *hotReplicas > 0 {
		hotReplicasNum = *hotReplicas
	}

	hotStores := stores.NewStoresFromString(*hotStores, hotReplicasNum)
	hotReadStores := stores.NewStoresFromString(*hotReadStores, hotReplicasNum)
	readStores := stores.NewStoresFromString(*readStores, *replicas)
	writeStores := stores.NewStoresFromString(*writeStores, *replicas)

	logger.Info("stores data",
		zap.String("read_stores", readStores.String()),
		zap.String("write_stores", writeStores.String()),
		zap.String("hot_stores", hotStores.String()),
		zap.String("hot_read_stores", hotReadStores.String()),
	)

	err := validateIngestorTopology(hotStores, hotReadStores, readStores, writeStores)
	if err != nil {
		logger.Fatal("validating topology", zap.Error(err))
	}

	config := proxyapi.IngestorConfig{
		API: proxyapi.APIConfig{
			SearchTimeout:  consts.DefaultSearchTimeout,
			ExportTimeout:  consts.DefaultExportTimeout,
			QueryRateLimit: *queryRateLimit,
			EsVersion:      *esVersion,
			GatewayAddr:    *proxyGrpcAddr,
		},
		Search: search.Config{
			HotStores:       hotStores,
			HotReadStores:   hotReadStores,
			ReadStores:      readStores,
			WriteStores:     writeStores,
			ShuffleReplicas: *shuffleReplicas,
			MirrorAddr:      *mirrorAddr,
		},
		Bulk: bulk.IngestorConfig{
			HotStores:   hotStores,
			WriteStores: writeStores,
			BulkCircuit: circuitbreaker.Config{
				Timeout:                  *bulkShardTimeout,
				MaxConcurrent:            int64(*maxInflightBulks),
				NumBuckets:               *bulkBucketsCount,
				BucketWidth:              *bulkBucketWidth,
				RequestVolumeThreshold:   *bulkVolumeThreshold,
				ErrorThresholdPercentage: *bulkErrPercentage,
				SleepWindow:              *bulkSleepWindow,
			},
			MaxInflightBulks:       *maxInflightBulks,
			AllowedTimeDrift:       *allowedTimeDrift,
			FutureAllowedTimeDrift: *futureAllowedTimeDrift,
			MappingProvider:        mp,
			MaxTokenSize:           *maxTokenSize,
			CaseSensitive:          caseSensitive,
			PartialFieldIndexing:   *partialFieldIndexing,
			DocsZSTDCompressLevel:  *docsZSTDCompressLevel,
			MetasZSTDCompressLevel: *metasZSTDCompressLevel,
			MaxDocumentSize:        int(*maxDocSize),
		},
	}

	ingestor, err := proxyapi.NewIngestor(config, inMemory)
	if err != nil {
		logger.Panic("failed to init ingestor", zap.Error(err))
	}

	httpListener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("ingestor can't listen http addr", zap.String("http_addr", addr), zap.Error(err))
	}

	grpcListener, err := net.Listen("tcp", *proxyGrpcAddr)
	if err != nil {
		logger.Fatal("ingestor can't listen grpc addr", zap.String("grpc_addr", *proxyGrpcAddr), zap.Error(err))
	}

	ingestor.Start(httpListener, grpcListener)

	return ingestor
}

func startStore(ctx context.Context, addr string, mp storeapi.MappingProvider) *storeapi.Store {
	var configMode string
	if *storeMode == storeapi.StoreModeCold || *storeMode == storeapi.StoreModeHot {
		configMode = *storeMode
	}
	config := storeapi.StoreConfig{
		FracManager: fracmanager.Config{
			DataDir:           *dataDir,
			FracSize:          uint64(*fracSize),
			TotalSize:         uint64(*totalSize),
			CacheSize:         uint64(*cacheSize),
			FracLoadLimit:     0,
			ShouldReplay:      true,
			ShouldRemoveMeta:  true,
			MaintenanceDelay:  0,
			CacheGCDelay:      0,
			CacheCleanupDelay: 0,
			SealParams: frac.SealParams{
				IDsZstdLevel:           *sealCompressLevel,
				LIDsZstdLevel:          *sealCompressLevel,
				TokenListZstdLevel:     *sealCompressLevel,
				DocsPositionsZstdLevel: *sealCompressLevel,
				TokenTableZstdLevel:    *sealCompressLevel,
			},
		},
		API: storeapi.APIConfig{
			StoreMode: configMode,
			Bulk: storeapi.BulkConfig{
				RequestsLimit: *bulkRequestsLimit,
				LogThreshold:  time.Millisecond * time.Duration(*logBulkThresholdMs),
			},
			Search: storeapi.SearchConfig{
				WorkersCount:          *searchWorkersCount,
				MaxFractionHits:       *maxFractionHits,
				FractionsPerIteration: runtime.GOMAXPROCS(0),
				RequestsLimit:         *searchRequestsLimit,
				LogThreshold:          time.Millisecond * time.Duration(*logSearchThresholdMs),
				Aggregation: storeapi.AggregationsConfig{
					MaxGroupTokens:     *aggMaxGroupTokens,
					MaxFieldTokens:     *aggMaxFieldTokens,
					MaxTIDsPerFraction: *aggMaxTIDsPerFraction,
				},
			},
			Fetch: storeapi.FetchConfig{
				LogThreshold: time.Millisecond * time.Duration(*logFetchThresholdMs),
			},
		},
	}
	store, err := storeapi.NewStore(ctx, config, mp)
	if err != nil {
		logger.Fatal("initializing store", zap.Error(err))
	}

	if addr != "" {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			logger.Fatal("store can't listen grpc addr", zap.Error(err))
		}
		store.Start(lis)
	}

	return store
}

func enableIndexingForAllFields(mappingPath string) bool {
	return mappingPath == "auto"
}
