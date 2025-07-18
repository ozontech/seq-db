package main

import (
	"context"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/encoding"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/config"
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

func validateIngestorTopology(hotStores, hotReadStores, _, _ *stores.Stores) error {
	// TODO: on evidently wrong topology fail fast

	if len(hotStores.Shards) == 0 && len(hotReadStores.Shards) == 0 {
		logger.Warn("no hot stores are set")
	}

	return nil
}

func main() {
	kingpin.Parse()
	kingpin.Version(buildinfo.Version)

	rand.Seed(time.Now().UnixNano())
	runtime.SetMutexProfileFraction(5)
	encoding.RegisterCodec(grpcutil.VTProtoCodec{})

	logger.Info("hi, I am seq-db",
		zap.String("version", buildinfo.Version),
		zap.String("build_time", buildinfo.BuildTime),
	)

	logger.Info(
		"value of GOMAXPROCS",
		zap.Int("GOMAXPROCS", runtime.GOMAXPROCS(0)),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.Setenv("TZ", "UTC"); err != nil {
		logger.Fatal("can't set timezone to UTC", zap.Error(err))
	}

	cfg, err := config.Parse(*flagConfig)
	if err != nil {
		logger.Fatal("cannot parse config file", zap.Error(err))
	}

	config.ReaderWorkers = cfg.Resources.ReaderWorkers
	config.CaseSensitive = cfg.Indexing.CaseSensitive
	config.SkipFsync = cfg.Resources.SkipFsync
	config.MaxRequestedDocuments = cfg.Limits.SearchDocs
	config.UseSeqQLByDefault = *flagUseSeqQLByDefault

	backoff.DefaultConfig.MaxDelay = 10 * time.Second

	var serviceReady atomic.Bool
	debugServer := debugserver.New(cfg.Address.Debug, &serviceReady)
	go debugServer.Start()

	var (
		store    *storeapi.Store
		ingestor *proxyapi.Ingestor
	)

	logger.Info("max queries per second", zap.Float64("limit", cfg.Limits.QueryRate))

	if err := tracing.Start(cfg.Tracing.SamplingRate); err != nil {
		logger.Error("error initializing tracing", zap.Error(err))
	}

	mappingProvider, err := mappingprovider.New(
		cfg.Mapping.Path,
		mappingprovider.WithUpdatePeriod(cfg.Mapping.UpdatePeriod),
		mappingprovider.WithIndexAllFields(enableIndexingForAllFields(cfg.Mapping.Path)),
	)
	if err != nil {
		logger.Fatal("load mapping error", zap.Error(err))
	}

	if cfg.Mapping.EnableUpdates {
		mappingProvider.WatchUpdates(ctx)
	}

	switch mode := *flagMode; mode {
	case appModeStore:
		store = startStore(ctx, mode, cfg, mappingProvider)
	case appModeProxy:
		ingestor = startProxy(ctx, cfg, mappingProvider, nil)
	case appModeSingle:
		store = startStore(ctx, mode, cfg, mappingProvider)
		ingestor = startProxy(ctx, cfg, mappingProvider, store)
	default:
		logger.Fatal("unknown mode", zap.String("mode", *flagMode))
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

func startProxy(
	_ context.Context,
	cfg config.Config, mp bulk.MappingProvider,
	inMemory *storeapi.Store,
) *proxyapi.Ingestor {
	logger.Info("max queries per second", zap.Float64("limit", cfg.Limits.QueryRate))

	hotReplicasNum := cfg.Cluster.Replicas
	if cfg.Cluster.HotReplicas > 0 {
		hotReplicasNum = cfg.Cluster.HotReplicas
	}

	hotStores := stores.NewStoresFromString(strings.Join(cfg.Cluster.HotStores, ","), hotReplicasNum)
	hotReadStores := stores.NewStoresFromString(strings.Join(cfg.Cluster.HotReadStores, ","), hotReplicasNum)
	readStores := stores.NewStoresFromString(strings.Join(cfg.Cluster.ReadStores, ","), cfg.Cluster.Replicas)
	writeStores := stores.NewStoresFromString(strings.Join(cfg.Cluster.WriteStores, ","), cfg.Cluster.Replicas)

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

	pconfig := proxyapi.IngestorConfig{
		API: proxyapi.APIConfig{
			SearchTimeout:  consts.DefaultSearchTimeout,
			ExportTimeout:  consts.DefaultExportTimeout,
			QueryRateLimit: cfg.Limits.QueryRate,
			EsVersion:      cfg.API.ESVersion,
			GatewayAddr:    cfg.Address.GRPC,
		},
		Search: search.Config{
			HotStores:       hotStores,
			HotReadStores:   hotReadStores,
			ReadStores:      readStores,
			WriteStores:     writeStores,
			ShuffleReplicas: cfg.Cluster.ShuffleReplicas,
			MirrorAddr:      cfg.Cluster.MirrorAddress,
		},
		Bulk: bulk.IngestorConfig{
			HotStores:   hotStores,
			WriteStores: writeStores,
			BulkCircuit: circuitbreaker.Config{
				Timeout:                  cfg.CircuitBreaker.Bulk.ShardTimeout,
				MaxConcurrent:            int64(cfg.Limits.InflightBulks),
				NumBuckets:               cfg.CircuitBreaker.Bulk.BucketsCount,
				BucketWidth:              cfg.CircuitBreaker.Bulk.BucketWidth,
				RequestVolumeThreshold:   int64(cfg.CircuitBreaker.Bulk.VolumeThreshold),
				ErrorThresholdPercentage: int64(cfg.CircuitBreaker.Bulk.ErrPercentage),
				SleepWindow:              cfg.CircuitBreaker.Bulk.SleepWindow,
			},
			MaxInflightBulks:       cfg.Limits.InflightBulks,
			AllowedTimeDrift:       cfg.Indexing.AllowedTimeDrift,
			FutureAllowedTimeDrift: cfg.Indexing.FutureAllowedTimeDrift,
			MappingProvider:        mp,
			MaxTokenSize:           cfg.Indexing.MaxTokenSize,
			CaseSensitive:          cfg.Indexing.CaseSensitive,
			PartialFieldIndexing:   cfg.Indexing.PartialFieldIndexing,
			DocsZSTDCompressLevel:  cfg.Compression.DocsZstdCompressionLevel,
			MetasZSTDCompressLevel: cfg.Compression.MetasZstdCompressionLevel,
			MaxDocumentSize:        int(cfg.Limits.DocSize),
		},
	}

	ingestor, err := proxyapi.NewIngestor(pconfig, inMemory)
	if err != nil {
		logger.Panic("failed to init ingestor", zap.Error(err))
	}

	httpListener, err := net.Listen("tcp", cfg.Address.HTTP)
	if err != nil {
		logger.Fatal("ingestor can't listen http addr", zap.String("http_addr", cfg.Address.HTTP), zap.Error(err))
	}

	grpcListener, err := net.Listen("tcp", cfg.Address.GRPC)
	if err != nil {
		logger.Fatal("ingestor can't listen grpc addr", zap.String("grpc_addr", cfg.Address.GRPC), zap.Error(err))
	}

	ingestor.Start(httpListener, grpcListener)

	return ingestor
}

func startStore(
	ctx context.Context, mode string,
	cfg config.Config, mp storeapi.MappingProvider,
) *storeapi.Store {
	var configMode string
	if *flagStoreMode == storeapi.StoreModeCold || *flagStoreMode == storeapi.StoreModeHot {
		configMode = *flagStoreMode
	}

	sconfig := storeapi.StoreConfig{
		FracManager: fracmanager.Config{
			DataDir:           cfg.Storage.DataDir,
			FracSize:          uint64(cfg.Storage.FracSize),
			TotalSize:         uint64(cfg.Storage.TotalSize),
			CacheSize:         uint64(cfg.Resources.CacheSize),
			SortCacheSize:     uint64(cfg.Resources.SortDocsCacheSize),
			FracLoadLimit:     0,
			ShouldReplay:      true,
			MaintenanceDelay:  0,
			CacheGCDelay:      0,
			CacheCleanupDelay: 0,
			SealParams: frac.SealParams{
				IDsZstdLevel:           cfg.Compression.SealedZstdCompressionLevel,
				LIDsZstdLevel:          cfg.Compression.SealedZstdCompressionLevel,
				TokenListZstdLevel:     cfg.Compression.SealedZstdCompressionLevel,
				DocsPositionsZstdLevel: cfg.Compression.SealedZstdCompressionLevel,
				TokenTableZstdLevel:    cfg.Compression.SealedZstdCompressionLevel,
				DocBlocksZstdLevel:     cfg.Compression.DocBlockZstdCompressionLevel,
				DocBlockSize:           int(cfg.DocsSorting.DocBlockSize),
			},
			Fraction: frac.Config{
				Search: frac.SearchConfig{
					AggLimits: frac.AggLimits{
						MaxFieldTokens:     cfg.Limits.Aggregation.FieldTokens,
						MaxGroupTokens:     cfg.Limits.Aggregation.GroupTokens,
						MaxTIDsPerFraction: cfg.Limits.Aggregation.FractionTokens,
					},
				},
				SkipSortDocs: !cfg.DocsSorting.Enabled,
				KeepMetaFile: false,
			},
		},
		API: storeapi.APIConfig{
			StoreMode: configMode,
			Bulk: storeapi.BulkConfig{
				RequestsLimit: uint64(cfg.Limits.BulkRequests),
				LogThreshold:  cfg.SlowLogs.BulkThreshold,
			},
			Search: storeapi.SearchConfig{
				WorkersCount:          cfg.Resources.SearchWorkers,
				MaxFractionHits:       cfg.Limits.FractionHits,
				FractionsPerIteration: config.NumCPU,
				RequestsLimit:         uint64(cfg.Limits.SearchRequests),
				LogThreshold:          cfg.SlowLogs.SearchThreshold,
				Async: fracmanager.AsyncSearcherConfig{
					DataDir:     cfg.AsyncSearch.DataDir,
					Parallelism: cfg.AsyncSearch.Concurrency,
				},
			},
			Fetch: storeapi.FetchConfig{
				LogThreshold: cfg.SlowLogs.FetchThreshold,
			},
		},
	}
	store, err := storeapi.NewStore(ctx, sconfig, mp)
	if err != nil {
		logger.Fatal("initializing store", zap.Error(err))
	}

	if mode != appModeSingle {
		lis, err := net.Listen("tcp", cfg.Address.GRPC)
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
