package consts

import (
	"errors"
	"time"

	"github.com/alecthomas/units"
)

const (
	// DummyMID is used in aggregations when we do not need to build time series.
	DummyMID = 0

	IDsPerBlock        = int(4 * units.KiB)
	DefaultLIDBlockCap = int(64 * units.KiB)
	RegularBlockSize   = int(16 * units.KiB)

	DefaultMaintenanceDelay  = time.Second
	DefaultCacheGCDelay      = 1 * time.Second
	DefaultCacheCleanupDelay = 5 * time.Millisecond

	DefaultMaxTokenSize = 72

	DefaultBulkRequestsLimit   = 32
	DefaultSearchRequestsLimit = 32
	DefaultTokenFreqThreshold  = 50

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

	DefaultMinSealPercent = 20 // Percent of the max frac size, above which the fraction is sealed on exit

	IngestorMaxInflightBulks = 32

	DefaultReplayWorkers = 2

	// dir names
	BrokenDir = ".broken"

	// known extensions
	WalFileSuffix = ".wal"

	DocsFileSuffix    = ".docs"
	DocsTmpFileSuffix = "._docs"
	DocsDelFileSuffix = ".docs.del"

	SdocsFileSuffix    = ".sdocs"
	SdocsTmpFileSuffix = "._sdocs"
	SdocsDelFileSuffix = ".sdocs.del"

	InfoFileSuffix    = ".info"
	InfoTmpFileSuffix = "._info"

	TokenFileSuffix    = ".tokens"
	TokenTmpFileSuffix = "._tokens"

	OffsetsFileSuffix    = ".offsets"
	OffsetsTmpFileSuffix = "._offsets"

	IDFileSuffix    = ".ids"
	IDTmpFileSuffix = "._ids"

	LIDFileSuffix    = ".lids"
	LIDTmpFileSuffix = "._lids"

	// IndexFileSuffix is the legacy single-file index format (pre-split).
	IndexFileSuffix    = ".index"
	IndexTmpFileSuffix = "._index"
	// TODO(dkharms): [IndexDelFileSuffix] is actually not necessary.
	// We can remove it in the future releases.
	IndexDelFileSuffix = ".index.del"

	RemoteFractionSuffix = ".remote"

	FracCacheFileSuffix = ".frac-cache"
	CompactionPlan      = ".compaction-plan"

	// tracing
	JaegerDebugKey = "jaeger-debug-id"
	DebugHeader    = "x-o3-sample-trace"

	// StoreProtocolVersionHeader reports store protocol version
	StoreProtocolVersionHeader = "x-seq-protocol-id"
)

var (
	TimeFields  = [][]string{{"timestamp"}, {"time"}, {"ts"}}
	TimeFormats = []string{ESTimeFormat, time.RFC3339Nano, time.RFC3339}

	ErrPartialResponse           = errors.New("partial response: some shards returned error")
	ErrIngestorQueryWantsOldData = errors.New("query wants old data, i am hot store")
	ErrRequestWasRateLimited     = errors.New("request was rate limited")
	ErrInvalidAggQuery           = errors.New("invalid agg query")
	ErrInvalidArgument           = errors.New("invalid argument")
	ErrTooManyFieldTokens        = errors.New("aggregation has too many field tokens")
	ErrTooManyFieldValues        = errors.New("aggregation has too many field values in memory")
	ErrTooManyGroupTokens        = errors.New("aggregation has too many group tokens")
	ErrTooManyFractionTokens     = errors.New("aggregation has too many fraction tokens")
	ErrTooManyFractionsHit       = errors.New("too many fractions hit")
	ErrMemoryLimitExceeded       = errors.New("memory limit exceeded")
)
