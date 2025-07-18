package main

import (
	"context"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/encoding"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/limits"
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

	conf.ReaderWorkers = *flagReaderWorkers
	conf.CaseSensitive = *flagCaseSensitive
	conf.SkipFsync = *flagSkipFsync
	conf.MaxRequestedDocuments = *flagMaxSearchDocs
	conf.UseSeqQLByDefault = *flagUseSeqQLByDefault

	backoff.DefaultConfig.MaxDelay = 10 * time.Second

	var serviceReady atomic.Bool
	debugServer := debugserver.New(*flagDebugAddr, &serviceReady)
	go debugServer.Start()

	var (
		store    *storeapi.Store
		ingestor *proxyapi.Ingestor
	)

	logger.Info("max queries per second", zap.Float64("limit", *flagQueryRateLimit))

	if err := tracing.Start(*flagTracingProbability); err != nil {
		logger.Error("error initializing tracing", zap.Error(err))
	}

	mappingProvider, err := mappingprovider.New(
		*flagMappingPath,
		mappingprovider.WithUpdatePeriod(*flagMappingUpdatePeriod),
		mappingprovider.WithIndexAllFields(enableIndexingForAllFields(*flagMappingPath)),
	)
	if err != nil {
		logger.Fatal("load mapping error", zap.Error(err))
	}

	if *flagEnableMappingUpdates {
		mappingProvider.WatchUpdates(ctx)
	}

	switch *flagMode {
	case appModeStore:
		store = startStore(ctx, *flagAddr, mappingProvider)
	case appModeProxy:
		ingestor = startProxy(ctx, *flagAddr, mappingProvider, nil)
	case appModeSingle:
		store = startStore(ctx, "", mappingProvider)
		ingestor = startProxy(ctx, *flagAddr, mappingProvider, store)
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

func startProxy(_ context.Context, addr string, mp bulk.MappingProvider, inMemory *storeapi.Store) *proxyapi.Ingestor {
	logger.Info("max queries per second", zap.Float64("limit", *flagQueryRateLimit))

	hotReplicasNum := *flagReplicas
	if *flagHotReplicas > 0 {
		hotReplicasNum = *flagHotReplicas
	}

	hotStores := stores.NewStoresFromString(*flagHotStores, hotReplicasNum)
	hotReadStores := stores.NewStoresFromString(*flagHotReadStores, hotReplicasNum)
	readStores := stores.NewStoresFromString(*flagReadStores, *flagReplicas)
	writeStores := stores.NewStoresFromString(*flagWriteStores, *flagReplicas)

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
			QueryRateLimit: *flagQueryRateLimit,
			EsVersion:      *flagEsVersion,
			GatewayAddr:    *flagProxyGrpcAddr,
		},
		Search: search.Config{
			HotStores:       hotStores,
			HotReadStores:   hotReadStores,
			ReadStores:      readStores,
			WriteStores:     writeStores,
			ShuffleReplicas: *flagShuffleReplicas,
			MirrorAddr:      *flagMirrorAddr,
		},
		Bulk: bulk.IngestorConfig{
			HotStores:   hotStores,
			WriteStores: writeStores,
			BulkCircuit: circuitbreaker.Config{
				Timeout:                  *flagBulkShardTimeout,
				MaxConcurrent:            int64(*flagMaxInflightBulks),
				NumBuckets:               *flagBulkBucketsCount,
				BucketWidth:              *flagBulkBucketWidth,
				RequestVolumeThreshold:   *flagBulkVolumeThreshold,
				ErrorThresholdPercentage: *flagBulkErrPercentage,
				SleepWindow:              *flagBulkSleepWindow,
			},
			MaxInflightBulks:       *flagMaxInflightBulks,
			AllowedTimeDrift:       *flagAllowedTimeDrift,
			FutureAllowedTimeDrift: *flagFutureAllowedTimeDrift,
			MappingProvider:        mp,
			MaxTokenSize:           *flagMaxTokenSize,
			CaseSensitive:          *flagCaseSensitive,
			PartialFieldIndexing:   *flagPartialFieldIndexing,
			DocsZSTDCompressLevel:  *flagDocsZstdCompressLevel,
			MetasZSTDCompressLevel: *flagMetasZstdCompressLevel,
			MaxDocumentSize:        int(*flagMaxDocSize),
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

	grpcListener, err := net.Listen("tcp", *flagProxyGrpcAddr)
	if err != nil {
		logger.Fatal("ingestor can't listen grpc addr", zap.String("grpc_addr", *flagProxyGrpcAddr), zap.Error(err))
	}

	ingestor.Start(httpListener, grpcListener)

	return ingestor
}

func startStore(ctx context.Context, addr string, mp storeapi.MappingProvider) *storeapi.Store {
	var configMode string
	if *flagStoreMode == storeapi.StoreModeCold || *flagStoreMode == storeapi.StoreModeHot {
		configMode = *flagStoreMode
	}
	config := storeapi.StoreConfig{
		FracManager: fracmanager.Config{
			DataDir:           *flagDataDir,
			FracSize:          uint64(*flagFracSize),
			TotalSize:         uint64(*flagTotalSize),
			CacheSize:         uint64(*flagCacheSize),
			SortCacheSize:     uint64(*flagSortCacheSize),
			FracLoadLimit:     0,
			ShouldReplay:      true,
			MaintenanceDelay:  0,
			CacheGCDelay:      0,
			CacheCleanupDelay: 0,
			SealParams: frac.SealParams{
				IDsZstdLevel:           *flagZstdSealCompressLevel,
				LIDsZstdLevel:          *flagZstdSealCompressLevel,
				TokenListZstdLevel:     *flagZstdSealCompressLevel,
				DocsPositionsZstdLevel: *flagZstdSealCompressLevel,
				TokenTableZstdLevel:    *flagZstdSealCompressLevel,
				DocBlocksZstdLevel:     *flagDocBlocksZstdCompressLevel,
				DocBlockSize:           int(*flagDocBlockSize),
			},
			Fraction: frac.Config{
				Search: frac.SearchConfig{
					AggLimits: frac.AggLimits{
						MaxFieldTokens:     *flagAggMaxFieldTokens,
						MaxGroupTokens:     *flagAggMaxGroupTokens,
						MaxTIDsPerFraction: *flagAggMaxTIDsPerFraction,
					},
				},
				SkipSortDocs: !*flagSortDocs,
				KeepMetaFile: false,
			},
		},
		API: storeapi.APIConfig{
			StoreMode: configMode,
			Bulk: storeapi.BulkConfig{
				RequestsLimit: *flagBulkRequestsLimit,
				LogThreshold:  time.Millisecond * time.Duration(*flagLogBulkThresholdMs),
			},
			Search: storeapi.SearchConfig{
				WorkersCount:          *flagSearchWorkers,
				MaxFractionHits:       *flagMaxFractionHits,
				FractionsPerIteration: limits.NumCPU,
				RequestsLimit:         *flagSearchRequestsLimit,
				LogThreshold:          time.Millisecond * time.Duration(*flagLogSearchThresholdMs),
				Async: fracmanager.AsyncSearcherConfig{
					DataDir:           *flagAsyncSearchesDataDir,
					Workers:           *flagAsyncSearchesWorkers,
					MaxSize:           *flagAsyncSearchesMaxSize,
					MaxSizePerRequest: *flagAsyncSearchesMaxSizePerRequest,
				},
			},
			Fetch: storeapi.FetchConfig{
				LogThreshold: time.Millisecond * time.Duration(*flagLogFetchThresholdMs),
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
