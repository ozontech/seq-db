package main

import (
	"strconv"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/limits"
	"github.com/ozontech/seq-db/storeapi"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	appModeProxy  = "proxy"
	appModeStore  = "store"
	appModeSingle = "single"

	defaultCacheSizeRatio = 0.3
)

var (
	numCPUStr = strconv.Itoa(limits.NumCPU)

	defaultCacheSize    = float64(limits.TotalMemory) * defaultCacheSizeRatio
	defaultCacheSizeStr = strconv.Itoa(int(defaultCacheSize)/1024/1024) + "MB"

	flagMode = kingpin.Flag("mode", `operation mode`).Default(appModeSingle).HintOptions(appModeSingle, appModeProxy, appModeStore).String()

	// addresses
	flagAddr          = kingpin.Flag("addr", `listen addr e.g. ":9002"`).Default(":9002").String()
	flagProxyGrpcAddr = kingpin.Flag("proxy-grpc-addr", `listen addr for grpc e.g. ":9004"`).Default(":9004").String()
	flagDebugAddr     = kingpin.Flag("debug-addr", `debug listen addr e.g. ":9200"`).Default(":9200").String()

	// cluster
	flagReadStores  = kingpin.Flag("read-stores", `list of store hosts to read`).String()
	flagWriteStores = kingpin.Flag("write-stores", `list of store hosts to write`).String()
	flagReplicas    = kingpin.Flag("replicas", `replication factor for stores`).Default("1").Int()

	flagHotStores     = kingpin.Flag("hot-stores", `list of hot store hosts`).String()
	flagHotReadStores = kingpin.Flag("hot-read-stores", `list of hot stores to read`).String()
	flagHotReplicas   = kingpin.Flag("hot-replicas", `replication factor for hot stores (if not set, global is used)`).Int()

	flagStoreMode       = kingpin.Flag("store-mode", `store operation mode`).HintOptions("", storeapi.StoreModeCold, storeapi.StoreModeHot).Default("").String()
	flagShuffleReplicas = kingpin.Flag("shuffle-replicas", `shuffle replicas before performing search`).Default("false").Bool()

	// dataset
	flagDataDir   = kingpin.Flag("data-dir", `directory to load/store data`).ExistingDir()
	flagFracSize  = kingpin.Flag("frac-size", `size of one fraction`).Default("128MB").Bytes()
	flagTotalSize = kingpin.Flag("total-size", `max size of all data`).Default("1GB").Bytes()

	// slow logs
	flagLogBulkThresholdMs   = kingpin.Flag("log-bulk-threshold-ms", `threshold for logging bulk requests, ms`).Default("0").Int()
	flagLogSearchThresholdMs = kingpin.Flag("log-search-threshold-ms", `threshold for logging search queries, ms`).Default("3000").Int()
	flagLogFetchThresholdMs  = kingpin.Flag("log-fetch-threshold-ms", `threshold for logging fetch requests, ms`).Default("3000").Int()

	// limits
	flagQueryRateLimit      = kingpin.Flag("query-rate-limit", `max requests per second`).Default("2.0").Float()
	flagBulkRequestsLimit   = kingpin.Flag("requests-limit", `maximum number of simultaneous bulk requests`).Default(strconv.Itoa(consts.DefaultBulkRequestsLimit)).Uint64()
	flagMaxInflightBulks    = kingpin.Flag("max-inflight-bulks", `max ingestor inflight bulk requests`).Default(strconv.Itoa(consts.IngestorMaxInflightBulks)).Int()
	flagSearchRequestsLimit = kingpin.Flag("search-requests-limit", `maximum number of simultaneous search requests`).Default(strconv.Itoa(consts.DefaultSearchRequestsLimit)).Uint64()
	flagMaxFractionHits     = kingpin.Flag("search-fraction-limit", `the maximum number of fractions used in the search`).Default("6000").Int()
	flagMaxSearchDocs       = kingpin.Flag("max-search-docs", `maximum number of documents returned by search query`).Default(strconv.Itoa(conf.MaxRequestedDocuments)).Int()
	flagMaxDocSize          = kingpin.Flag("max-document-size", "the maximum document size, documents larger than this will be skipped").Default("128KiB").Bytes()

	// aggregation limits
	flagAggMaxGroupTokens     = kingpin.Flag("agg-max-group-tokens", `the maximum group tokens per aggregation, set 0 to disable limit`).Default("2000").Int()
	flagAggMaxFieldTokens     = kingpin.Flag("agg-max-field-tokens", `the maximum field tokens per aggregation, set 0 to disable limit`).Default("1000000").Int()
	flagAggMaxTIDsPerFraction = kingpin.Flag("agg-max-fraction-tids", `the number of max unique tokens per fraction, set 0 to disable limit`).Default("100000").Int()

	// proxy circuitbreaker for bulk
	flagBulkShardTimeout    = kingpin.Flag("bulk-shard-timeout", `timeout for a shard to process bulk`).Default("10s").Duration()
	flagBulkErrPercentage   = kingpin.Flag("bulk-err-percentage", "check circuitbreaker/README.md for more details").Default("50").Int64()
	flagBulkBucketWidth     = kingpin.Flag("bulk-bucket-width", "check circuitbreaker/README.md for more details").Default("1s").Duration()
	flagBulkBucketsCount    = kingpin.Flag("bulk-err-count", "check circuitbreaker/README.md for more details").Default("10").Int()
	flagBulkSleepWindow     = kingpin.Flag("bulk-sleep-window", "check circuitbreaker/README.md for more details").Default("5s").Duration()
	flagBulkVolumeThreshold = kingpin.Flag("bulk-request-volume-threshold", "check circuitbreaker/README.md for more details").Default("5").Int64()

	// resources
	flagReaderWorkers = kingpin.Flag("reader-workers", "size of readers pool").Default(numCPUStr).Int()
	flagSearchWorkers = kingpin.Flag("search-workers-count", `the number of workers that will be process factions`).Default(numCPUStr).Int()
	flagSkipFsync     = kingpin.Flag("skip-fsync", "skip fsyncs for active fraction").Default("false").Bool()
	flagCacheSize     = kingpin.Flag("cache-size", `max size of the cache`).Default(defaultCacheSizeStr).Bytes()
	flagSortCacheSize = kingpin.Flag("sdocs-cache-size", `cache size that used to seal active fraction, must be lower than --cache-size parameter`).Bytes()

	// compress level
	flagDocsZstdCompressLevel      = kingpin.Flag("docs-zstd-compress-level", `ZSTD compress level for docs, change these parameters if you need to change the network load, does not affect the final size of documents or index on disk, check the doc for more details: https://facebook.github.io/zstd/zstd_manual.html`).Default("1").Int()
	flagMetasZstdCompressLevel     = kingpin.Flag("metas-zstd-compress-level", `ZSTD compress level for metas, change these parameters if you need to change the network load, does not affect the final size of documents or index on disk, check the doc for more details: https://facebook.github.io/zstd/zstd_manual.html`).Default("1").Int()
	flagZstdSealCompressLevel      = kingpin.Flag("seal-zstd-compress-level", "ZSTD compress level that will be used on seal index file: https://facebook.github.io/zstd/zstd_manual.html").Default("3").Int()
	flagDocBlocksZstdCompressLevel = kingpin.Flag("doc-block-zstd-compress-level", `ZSTD compress level for document blocks, check the doc for more details: https://facebook.github.io/zstd/zstd_manual.html`).Default("3").Int()

	// indexing
	flagMaxTokenSize           = kingpin.Flag("max-token-size", ``).Default("72").Int()
	flagCaseSensitive          = kingpin.Flag("case-sensitive", "case insensitive to tokens").Default("false").Bool()
	flagPartialFieldIndexing   = kingpin.Flag("partial-indexing", `index only the first part of long fields`).Default("false").Bool()
	flagAllowedTimeDrift       = kingpin.Flag("allowed-time-drift", `maximum allowed time since the message timestamp`).Default("24h").Duration()
	flagFutureAllowedTimeDrift = kingpin.Flag("future-allowed-time-drift", `maximum future allowed time since the message timestamp`).Default("5m").Duration()

	// mapping
	flagMappingPath          = kingpin.Flag("mapping", `path to mapping file or 'auto' to index all fields`).Required().String()
	flagEnableMappingUpdates = kingpin.Flag("enable-mapping-updates", "this will periodically check mapping file and reload configuration if there is an update").Default("false").Bool()
	flagMappingUpdatePeriod  = kingpin.Flag("mapping-update-period", "the amount of time to pass for the mappings to be reloaded").Default("30s").Duration()

	// sort docs settings
	flagDocBlockSize = kingpin.Flag("doc-block-size", "document block size, large size consumes more RAM but improves compression ratio").Default("4MiB").Bytes()
	flagSortDocs     = kingpin.Flag("sort-docs", "enable sort docs feature").Default("true").Bool()

	// async search settings
	flagAsyncSearchesDataDir           = kingpin.Flag("data-dir-async-searches", "data dir that contains async searches, default is subfolder in --data-dir").String()
	flagAsyncSearchesWorkers           = kingpin.Flag("async-searches-workers", "the maximum concurrent async search requests").Default(numCPUStr).Int()
	flagAsyncSearchesMaxSize           = kingpin.Flag("async-searches-max-size", "").Default("0").Int()
	flagAsyncSearchesMaxSizePerRequest = kingpin.Flag("async-searches-max-size-per-request", "").Default("0").Int()

	// features
	flagUseSeqQLByDefault = kingpin.Flag("use-seq-ql-by-default", "enable seq-ql as default query language").Default("false").Bool()

	// others
	flagEsVersion          = kingpin.Flag("es-version", "ES version to return in the `/` handler").Default("8.9.0").String()
	flagMirrorAddr         = kingpin.Flag("mirror-addr", `the address of the seqproxy mirror`).Default("").String()
	flagTracingProbability = kingpin.Flag("tracing-probability", ``).Default("0.01").Float64()

	// Deprecated: do not use.
	_ = kingpin.Flag("max-pool-size", ``).Default("1024").Hidden().Int()
	_ = kingpin.Flag("max-buffer-cap", ``).Default("32768").Hidden().Int()
)
