package consts

import (
	"errors"
	"time"
)

const (
	IDsBlockSize     = 4 * KB
	RegularBlockSize = 16 * KB
	IDsPerBlock      = 4 * KB

	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	DefaultMaintenanceDelay  = time.Second
	DefaultCacheGCDelay      = 10 * time.Second
	DefaultCacheCleanupDelay = 50 * time.Millisecond

	// indexing
	LIDBlockCap                = 64 * KB
	DefaultMaxTokenSize        = 72
	DefaultBulkRequestsLimit   = 16
	DefaultSearchRequestsLimit = 30

	// search
	DefaultComputeN        = 2 * KB
	DefaultMaxFractionHits = 6000

	DefaultMaxGroupsPerAggregation       = 2_000
	DefaultMaxFieldTokensPerAggregation  = 1_000_000
	DefaultMaxAggregatingTIDsPerFraction = 100_000

	// bulk
	BulkMaxTries = 3

	IngestorMaxInstances = 1024 // should be power of two

	ESTimeFormat = "2006-01-02 15:04:05.999"

	BulkTimeout          = 30 * time.Second
	DefaultSearchTimeout = 30 * time.Second
	DefaultExportTimeout = 2 * time.Minute

	GRPCServerShutdownTimeout = 10 * time.Second

	ProxyBulkStatsInterval = time.Second * 5

	// query starting from this requests to be explained
	QueryExplainPrefix = "EXPLAIN "
	QueryExplainString = "EXPLAIN"

	MirrorRequestLimit = 300

	ErrStringQueryWantsOldData = "query wants old data, i am hot store"

	// DefaultMaxRequestedDocuments is the maximum number of documents that can be requested in one fetch request
	DefaultMaxRequestedDocuments = 100_000

	MaxTextFieldValueLength        = 32 * 1024
	MaxKeywordListFieldValueLength = 128 * 1024

	SealOnExitFracSizePercent = 20 // Percent of the max frac size, above which the fraction is sealed on exit

	MinMergeQueue = 10000
)

const (
	DocsFileSuffix      = ".docs"
	MetaFileSuffix      = ".meta"
	DocsDelFileSuffix   = ".docs.del"
	IndexTmpFileSuffix  = "._index"
	IndexFileSuffix     = ".index"
	IndexDelFileSuffix  = ".index.del"
	FracCacheFileSuffix = ".frac-cache"
)

const (
	IngestorMaxInflightBulks = 32
	ReaderWorkers            = 128
)

var (
	TimeFields  = [][]string{{"timestamp"}, {"time"}, {"ts"}}
	TimeFormats = []string{ESTimeFormat, time.RFC3339Nano, time.RFC3339}
)

const (
	AllowedTimeDrift       = "24h"
	FutureAllowedTimeDrift = "5m"
)

var (
	ErrUnexpectedInterruption    = errors.New("unexpected interruption")
	ErrPartialResponse           = errors.New("partial response: some shards returned error")
	ErrIngestorQueryWantsOldData = errors.New(ErrStringQueryWantsOldData)
	ErrStoreQueryWantsOldData    = errors.New(ErrStringQueryWantsOldData)
	ErrRequestWasRateLimited     = errors.New("request was rate limited")
	ErrInvalidAggQuery           = errors.New("invalid agg query")
	ErrInvalidArgument           = errors.New("invalid argument")
	ErrTooManyUniqValues         = errors.New("aggregation has too many unique values")
)

const (
	JaegerDebugKey = "jaeger-debug-id"
	DebugHeader    = "x-o3-sample-trace"
)
