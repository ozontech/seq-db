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
		FracSize Bytes `config:"frac_size" default:"128MiB"`
		// TotalSize specifies upper bound of how much disk space can be occupied
		// by sealed fractions before they get deleted (or offloaded).
		TotalSize Bytes `config:"total_size" default:"1GiB"`
	} `config:"storage"`

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

		Aggregation struct {
			// FieldTokens specifies maximum amount of unique field tokens
			// that can be processed in single aggregation requests.
			// Setting this field to 0 disables limit.
			FieldTokens int `config:"field_tokens" default:"1000000"`
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
		DocBlockSize Bytes `config:"doc_block_size"`
	} `config:"docs_sorting"`

	AsyncSearch struct {
		// DataDir specifies directory that contains data for asynchronous searches.
		// By default will be subdirectory in [Config.Storage.DataDir].
		DataDir     string `config:"data_dir"`
		Concurrency int    `config:"concurrency"`
	} `config:"async_search"`

	API struct {
		// EsVersion is the default version that will be returned in the `/` handler.
		ESVersion string `config:"es_version" default:"8.9.0"`
	} `config:"api"`

	Tracing struct {
		SamplingRate float64 `config:"sampling_rate" default:"0.01"`
	} `config:"tracing"`
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
