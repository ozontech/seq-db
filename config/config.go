package config

import (
	"cmp"
	"path/filepath"
	"time"

	"github.com/alecthomas/units"
	"github.com/kkyr/fig"
)

const (
	defaultCacheSizeRatio = 0.3
)

func Parse(path string) (Config, error) {
	var c Config

	abs, err := filepath.Abs(path)
	if err != nil {
		return Config{}, err
	}

	if err := fig.Load(
		&c,
		fig.File(filepath.Base(abs)),
		// To find config file [fig] iterates over directories
		// and concatenates filepath with each directory.
		fig.Dirs(filepath.Dir(abs)),
		fig.UseStrict(),
		fig.Tag("config"),
		fig.UseEnv("SEQDB"),
	); err != nil {
		return Config{}, err
	}

	/* Set computed defaults if user did not override them */
	c.Compaction.Workers = cmp.Or(c.Compaction.Workers, NumCPU)

	c.Resources.ReaderWorkers = cmp.Or(c.Resources.ReaderWorkers, NumCPU)
	c.Resources.SearchWorkers = cmp.Or(c.Resources.SearchWorkers, NumCPU)
	c.Resources.CacheSize = cmp.Or(c.Resources.CacheSize, Bytes(float64(TotalMemory)*defaultCacheSizeRatio))

	c.AsyncSearch.Concurrency = cmp.Or(c.AsyncSearch.Concurrency, NumCPU)

	return c, nil
}

type Config struct {
	Address struct {
		// HTTP listen address.
		HTTP string `config:"http" default:":9002"`
		// GRPC listen address.
		GRPC string `config:"grpc" default:":9004"`
		// Debug listen address.
		Debug string `config:"debug" default:":9200"`
	} `config:"address"`

	Storage struct {
		// DataDir is a path to a directory where fractions will be stored.
		DataDir string `config:"data_dir"`
		// FracSize specifies the maximum size of an active fraction before it gets sealed.
		FracSize Bytes `config:"frac_size" default:"16MiB"`
		// TotalSize specifies upper bound of how much disk space can be occupied
		// by sealed fractions before they get deleted (or offloaded).
		TotalSize Bytes `config:"total_size" default:"1GiB"`
		// SealingQueueLen defines the maximum length of the sealing queue.
		// If the queue size exceeds this limit, writing to the store will be paused,
		// and bulk requests will start returning errors.
		// A value of zero disables this limit, allowing writes to proceed unconditionally.
		SealingQueueLen int `config:"sealing_queue_len" default:"10"`
	} `config:"storage"`

	Sealing struct {
		Tokens struct {
			// BlockSize sets max token block size in bytes.
			BlockSize Bytes `config:"block_size" default:"16KiB"`
			// FreqThresholdPercentage specifies the minimum posting-list length as a percentage
			// of the fraction's document count. For example, with 1_000_000 docs and FreqThresholdPercentage=1,
			// frequency is stored for tokens that appear in at least 10_000 documents.
			FreqThresholdPercentage float64 `config:"freq_threshold_percentage" default:"0.005"`
		} `config:"tokens"`

		Lids struct {
			// BlockSize sets max lids (postings) saved per LIDs block.
			BlockSize int `config:"block_size" default:"65536"`
			// BitmapThreshold specifies minimum number of LIDs in the lid list
			// which are serialized as bitmap. LIDs lists with more elements use bitmap encoding,
			// while smaller lists use delta encoding. Default value is 0 (disabled).
			BitmapThreshold int `config:"bitmap_threshold"`
		} `config:"lids"`
	} `config:"sealing"`

	Cluster struct {
		// WriteStores contains cold store instances which will be written to.
		WriteStores []string `config:"write_stores"`
		// ReadStores contains cold store instances wich will be queried from.
		ReadStores []string `config:"read_stores"`

		// HotStores contains store instances which will be written to and queried from.
		HotStores []string `config:"hot_stores"`
		// HotReadStores contains store instances which will be queried from.
		// This field is optional but if specified will take precedence over [Proxy.Cluster.HotStores].
		HotReadStores []string `config:"hot_read_stores"`

		// Replicas specifies number of instances that belong to one shard.
		Replicas int `config:"replicas" default:"1"`
		// HotReplicas specifies number if hot instances that belong to one shard.
		// If specified will take precedence over [Replicas] for hot stores.
		HotReplicas     int  `config:"hot_replicas"`
		ShuffleReplicas bool `config:"shuffle_replicas"`

		// MirrorAddress specifies host to which search queries will be mirrored.
		// It can be useful if you have development cluster and you want to have same search pattern
		// as you have on production cluster.
		MirrorAddress string `config:"mirror_address"`

		// FailPartialResponse specifies whether unavailability of any shard inside cluster
		// should fail search requests
		FailPartialResponse bool `config:"fail_partial_response"`
	} `config:"cluster"`

	SlowLogs struct {
		// BulkThreshold specifies duration to determine slow bulks.
		// When bulk request exceeds this threshold it will be logged.
		BulkThreshold time.Duration `config:"bulk_threshold" default:"0ms"`
		// SearchThreshold specifies duration to determine slow searches.
		// When search request exceeds this threshold it will be logged.
		SearchThreshold time.Duration `config:"search_threshold" default:"3s"`
		// FetchThreshold specifies duration to determine slow fetches.
		// When fetch request exceeds this threshold it will be logged.
		FetchThreshold time.Duration `config:"fetch_threshold" default:"3s"`
	} `config:"slow_logs"`

	Limits struct {
		// QueryRate specifies maximum amount of requests per second.
		QueryRate float64 `config:"query_rate" default:"2"`

		// SearchRequests specifies maximum amount of simultaneous requests per second.
		SearchRequests int `config:"search_requests" default:"32"`
		// BulkRequests specifies maximum amount of simultaneous requests per second.
		BulkRequests int `config:"bulk_requests" default:"32"`
		// InflightBulks specifies maximum amount of simultaneous requests per second.
		InflightBulks int `config:"inflight_bulks" default:"32"`

		// FractionHits specifies maximum amount of fractions that can be processed
		// within single search request.
		FractionHits int `config:"fraction_hits" default:"6000"`
		// SearchDocs specifies maximum amount of documents that can be returned
		// within single search request.
		SearchDocs int `config:"search_docs" default:"100000"`
		// DocSize specifies maximum possible size for single document.
		// Document larger than this threshold will be skipped.
		DocSize Bytes `config:"doc_size" default:"128KiB"`
		// QprMemoryUsage specifies maximum heap memory which a single QPR (query partial result)
		// can use in either store or proxy.
		QprMemoryUsage Bytes `config:"qpr_memory_usage" default:"0B"`

		Aggregation struct {
			// FieldTokens specifies maximum amount of unique field tokens
			// that can be processed in single aggregation requests.
			// Setting this field to 0 disables limit.
			FieldTokens int `config:"field_tokens" default:"1000000"`
			// FieldValues specifies maximum amount of unique field values
			// that partial aggregation results (buckets) can contain in single aggregation requests.
			// Setting this field to 0 disables limit.
			FieldValues int `config:"field_values" default:"1000000"`
			// GroupTokens specifies maximum amount of unique group tokens
			// that can be processed in single aggregation requests.
			// Setting this field to 0 disables limit.
			GroupTokens int `config:"group_tokens" default:"2000"`
			// FractionTokens specifies maximum amount of unique tokens
			// that are contained in single fraction which was picked up by aggregation request.
			// Setting this field to 0 disables limit.
			FractionTokens int `config:"fraction_tokens" default:"100000"`
		} `config:"aggregation"`
	} `config:"limits"`

	QueryOptimization struct {
		BatchExecution struct {
			Enabled bool `config:"enabled"`
			// CostThreshold is the minimum estimated non-batched execution cost required to enable batch-at-a-time query
			// evaluation. Suggestion is to use value which is greater than 3 x LID block size.
			CostThreshold int `config:"cost_threshold" default:"150000"`
		} `config:"batch_execution"`
		MaterializedColumnAgg struct {
			Enabled bool `config:"enabled"`
		} `config:"materialized_column_agg"`
	} `config:"query_optimization"`

	CircuitBreaker struct {
		Bulk struct {
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			ShardTimeout time.Duration `config:"shard_timeout" default:"10s"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			ErrPercentage int `config:"err_percentage" default:"50"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			BucketWidth time.Duration `config:"bucket_width" default:"1s"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			BucketsCount int `config:"buckets_count" default:"10"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			SleepWindow time.Duration `config:"sleep_window" default:"5s"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			VolumeThreshold int `config:"volume_threshold" default:"5"`
		} `config:"bulk"`
	} `config:"circuit_breaker"`

	Resources struct {
		// ReaderWorkers specifies number of workers for readers pool.
		// By default this setting is equal to [runtime.GOMAXPROCS].
		ReaderWorkers int `config:"reader_workers"`
		// SearchWorkers specifies number of workers for searchers pool.
		// By default this setting is equal to [runtime.GOMAXPROCS].
		SearchWorkers int `config:"search_workers"`
		// ReplayWorkers specifies number of workers.
		// By default this setting is equal to 2.
		ReplayWorkers int `config:"replay_workers" default:"2"`
		// CacheSize specifies maxium size of cache.
		// By default this setting is equal to 30% of available RAM.
		CacheSize         Bytes `config:"cache_size"`
		SortDocsCacheSize Bytes `config:"sort_docs_cache_size"`
		SkipFsync         bool  `config:"skip_fsync"`
	} `config:"resources"`

	Compression struct {
		DocsZstdCompressionLevel     int `config:"docs_zstd_compression_level" default:"1"`
		MetasZstdCompressionLevel    int `config:"metas_zstd_compression_level" default:"1"`
		SealedZstdCompressionLevel   int `config:"sealed_zstd_compression_level" default:"3"`
		DocBlockZstdCompressionLevel int `config:"doc_block_zstd_compression_level" default:"3"`
	} `config:"compression"`

	Compaction struct {
		STCS struct {
			// MergeTrigger is the minimum number of fractions that a bucket must
			// contain before it becomes eligible for compaction.
			MergeTrigger int `config:"merge_trigger" default:"4"`
			// MergeFanIn caps how many fractions are compacted from a single bucket
			// per compaction iteration.
			MergeFanIn int `config:"merge_fan_in" default:"32"`
			// MergeFanOutSize is the upper bound on the combined input index size of
			// a single merge. It limits how large a compacted fraction can grow.
			MergeFanOutSize Bytes `config:"merge_fan_out_size" default:"512MiB"`
			// BucketLowerbound and BucketUpperbound control bucket membership:
			// a fraction joins a bucket only if its size is within
			// [BucketLowerbound, BucketUpperbound] * avg(bucket).
			BucketLowerbound float64 `config:"bucket_lowerbound" default:"0.5"`
			BucketUpperbound float64 `config:"bucket_upperbound" default:"1.5"`
		} `config:"stcs"`
		// Enabled is the master switch for background compaction.
		// Compaction is disabled unless this is set to true.
		Enabled bool `config:"enabled"`
		// Workers specifies the number of executor workers performing merges
		// concurrently. By default this setting is equal to [runtime.GOMAXPROCS].
		Workers int `config:"workers"`
		// TimeWindow is the width of a time bin. Fractions are grouped into bins by
		// truncating their creation time.
		TimeWindow time.Duration `config:"time_window" default:"1h"`
		// TickInterval specifies how often the planner wakes up to pick a single
		// compaction task.
		TickInterval time.Duration `config:"tick_interval" default:"1s"`
	} `config:"compaction"`

	Indexing struct {
		MaxTokenSize         int  `config:"max_token_size" default:"72"`
		CaseSensitive        bool `config:"case_sensitive"`
		PartialFieldIndexing bool `config:"partial_field_indexing"`
		// PastAllowedTimeDrift specifies how much time can elapse since the message’s timestamp.
		// If more time than PastAllowedTimeDrift has passed since the message’s timestamp, the message's timestamp gets overwritten.
		PastAllowedTimeDrift time.Duration `config:"past_allowed_time_drift" default:"24h"`
		// FutureAllowedTimeDrift specifies the maximum allowable offset for a message’s timestamp into the future.
		// If a message’s timestamp is further in the future than FutureAllowedTimeDrift, it is overwritten.
		FutureAllowedTimeDrift time.Duration `config:"future_allowed_time_drift" default:"5m"`
	} `config:"indexing"`

	Mapping struct {
		// Path to mapping file or 'auto' to index all fields as keywords.
		Path string `config:"path"`
		// EnableUpdates will periodically check mapping file and reload configuration if there is an update.
		EnableUpdates bool `config:"enable_updates"`
		// UpdatePeriod manages how often mapping file will be checked for updates.
		UpdatePeriod time.Duration `config:"update_period" default:"30s"`
	} `config:"mapping"`

	DocsSorting struct {
		// Enabled enables/disables documents sorting.
		Enabled bool `config:"enabled"`
		// DocBlockSize sets document block size.
		// Large size consumes more RAM but improves compression ratio.
		DocBlockSize Bytes `config:"doc_block_size" default:"128KiB"`
	} `config:"docs_sorting"`

	Offloading struct {
		Enabled bool `config:"enabled"`
		// Retention sets TTL for [frac.Remote] fractions.
		// By default no retention is configured and all [frac.Remote] fractions are kept forever.
		Retention time.Duration `config:"retention"`

		// Endpoint configures S3 endpoint for S3 client.
		Endpoint string `config:"endpoint" default:"https://s3.us-east-1.amazonaws.com/"`
		// Bucket configures the name of S3 bucket where [frac.Remote] fractions will be stored.
		Bucket string `config:"bucket"`
		Region string `config:"region" default:"us-east-1"`

		// AccessKey configures S3 Access Key for S3 client.
		// You can learn more about access keys [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).
		AccessKey string `config:"access_key"`
		// SecretKey configures S3 Secret Key for S3 client.
		// You can learn more about secret keys [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).
		SecretKey string `config:"secret_key"`
		// Specifies the percentage of total local dataset size allocated to the offloading queue.
		// Note: When the queue overflows, the oldest fraction of data is automatically removed.
		// This automatic removal is disabled when set to zero.
		QueueSizePercent float64 `config:"queue_size_percent" default:"5"`
		// Delay duration between consecutive offloading retries
		RetryDelay time.Duration `config:"retry_delay" default:"2s"`
	} `config:"offloading"`

	AsyncSearch struct {
		// DataDir specifies directory that contains data for asynchronous searches.
		// By default will be subdirectory in [Config.Storage.DataDir].
		DataDir                string `config:"data_dir"`
		Concurrency            int    `config:"concurrency"`
		MaxTotalSize           Bytes  `config:"max_total_size" default:"1GiB"`
		MaxSizePerRequest      Bytes  `config:"max_size_per_request" default:"100MiB"`
		MaxDocumentsPerRequest int64  `config:"max_documents_per_request" default:"100000"`
	} `config:"async_search"`

	API struct {
		// EsVersion is the default version that will be returned in the `/` handler.
		ESVersion string `config:"es_version" default:"8.9.0"`
	} `config:"api"`

	Tracing struct {
		SamplingRate float64 `config:"sampling_rate" default:"0.01"`
	} `config:"tracing"`

	// Additional filtering options
	Filtering SkipMaskParams `config:"filtering"`

	SkipMaskManager struct {
		DataDir   string           `config:"data_dir"`
		Workers   int              `config:"workers" default:"1"`
		SkipMasks []SkipMaskParams `config:"skip_masks"`
		CacheSize Bytes            `config:"cache_size" default:"100MiB"`
	} `config:"skip_mask_manager"`

	// Experimental provides flags
	// For configuring experimental features.
	// We might add or remove flags without backwards compatibility guarantees.
	Experimental struct {
		// Specify how many tokens can be checked using regular expressions.
		// If zero then there is no limit.
		MaxRegexTokensCheck int `config:"max_regex_tokens_check" default:"0"`
		// If true, suitable ComplexSearch queries will be served through stream search implementation.
		UseStreamSearch bool `config:"use_stream_search"`
	} `config:"experimental"`
}

type SkipMaskParams struct {
	Query string    `config:"query"`
	From  time.Time `config:"from"`
	To    time.Time `config:"to"`
}

type Bytes units.Base2Bytes

func (b *Bytes) UnmarshalString(s string) error {
	bytes, err := units.ParseBase2Bytes(s)
	if err != nil {
		return err
	}
	*b = Bytes(bytes)
	return nil
}
