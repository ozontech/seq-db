package consts

import (
	"errors"
	"time"

	"github.com/alecthomas/units"
)

const (
	// DummyMID is used in aggregations when we do not need to build time series.
	DummyMID = 0

	IDsBlockSize     = int(4 * units.KiB)
	RegularBlockSize = int(16 * units.KiB)
	IDsPerBlock      = int(4 * units.KiB)
	LIDBlockCap      = int(64 * units.KiB)

	DefaultMaintenanceDelay  = time.Second
	DefaultCacheGCDelay      = 1 * time.Second
	DefaultCacheCleanupDelay = 5 * time.Millisecond

	DefaultMaxTokenSize = 72

	DefaultBulkRequestsLimit   = 32
	DefaultSearchRequestsLimit = 32

	BulkMaxTries = 3

	IngestorMaxInstances = 1024 // should be power of two

	ESTimeFormat = "2006-01-02 15:04:05.999"

	BulkTimeout          = 30 * time.Second
	DefaultSearchTimeout = 30 * time.Second
	DefaultExportTimeout = 2 * time.Minute

	GRPCServerShutdownTimeout = 10 * time.Second

	ProxyBulkStatsInterval = time.Second * 5

	MirrorRequestLimit = 300

	MaxTextFieldValueLength = 32 * 1024

	SealOnExitFracSizePercent = 20 // Percent of the max frac size, above which the fraction is sealed on exit

	IngestorMaxInflightBulks = 32

	// known extensions
	MetaFileSuffix = ".meta"

	DocsFileSuffix    = ".docs"
	DocsDelFileSuffix = ".docs.del"

	SdocsFileSuffix    = ".sdocs"
	SdocsTmpFileSuffix = "._sdocs"
	SdocsDelFileSuffix = ".sdocs.del"

	IndexFileSuffix    = ".index"
	IndexTmpFileSuffix = "._index"
	IndexDelFileSuffix = ".index.del"

	RemoteFractionSuffix = ".remote"

	FracCacheFileSuffix = ".frac-cache"

	// tracing
	JaegerDebugKey = "jaeger-debug-id"
	DebugHeader    = "x-o3-sample-trace"
)

var (
	TimeFields  = [][]string{{"timestamp"}, {"time"}, {"ts"}}
	TimeFormats = []string{ESTimeFormat, time.RFC3339Nano, time.RFC3339}

	ErrPartialResponse           = errors.New("partial response: some shards returned error")
	ErrIngestorQueryWantsOldData = errors.New("query wants old data, i am hot store")
	ErrRequestWasRateLimited     = errors.New("request was rate limited")
	ErrInvalidAggQuery           = errors.New("invalid agg query")
	ErrInvalidArgument           = errors.New("invalid argument")
	ErrTooManyUniqValues         = errors.New("aggregation has too many unique values")
	ErrTooManyFractionsHit       = errors.New("too many fractions hit")
)
