package config

import (
	"cmp"
	"time"

	"github.com/alecthomas/units"
	"github.com/kkyr/fig"
)

const (
	defaultCacheSizeRatio = 0.3
)

func Parse(path string) (Config, error) {
	var c Config
	if err := fig.Load(&c, fig.File(path)); err != nil {
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
		DataDir string `config:"dataDir"`
		// FracSize specifies the maximum size of an active fraction before it gets sealed.
		FracSize Bytes `config:"fracSize" default:"128MiB"`
		// TotalSize specifies upper bound of how much disk space can be occupied
		// by sealed fractions before they get deleted (or offloaded).
		TotalSize Bytes `config:"totalSize" default:"1GiB"`
	}

	Cluster struct {
		// WriteStores contains cold store instances which will be written to.
		WriteStores []string `config:"writeStores"`
		// ReadStores contains cold store instances wich will be queried from.
		ReadStores []string `config:"readStores"`

		// HotStores contains store instances which will be written to and queried from.
		HotStores []string `config:"hotStores"`
		// HotReadStores contains store instances which will be queried from.
		// This field is optional but if specified will take precedence over [Proxy.Cluster.HotStores].
		HotReadStores []string `config:"hotReadStores"`

		// Replicas specifies number of instances that belong to one shard.
		Replicas int `config:"replicas" default:"1"`
		// HotReplicas specifies number if hot instances that belong to one shard.
		// If specified will take precedence over [Replicas] for hot stores.
		HotReplicas     int  `config:"hotReplicas"`
		ShuffleReplicas bool `config:"shuffleReplicas"`

		// MirrorAddress specifies host to which search queries will be mirrored.
		// It can be useful if you have development cluster and you want to have same search pattern
		// as you have on production cluster.
		MirrorAddress string `config:"mirrorAddress"`
	} `config:"cluster"`

	SlowLogs struct {
		// BulkThreshold specifies duration to determine slow bulks.
		// When bulk request exceeds this threshold it will be logged.
		BulkThreshold time.Duration `config:"bulkThreshold" default:"0ms"`
		// SearchThreshold specifies duration to determine slow searches.
		// When search request exceeds this threshold it will be logged.
		SearchThreshold time.Duration `config:"searchThreshold" default:"3s"`
		// FetchThreshold specifies duration to determine slow fetches.
		// When fetch request exceeds this threshold it will be logged.
		FetchThreshold time.Duration `config:"fetchThreshold" default:"3s"`
	} `config:"slowLogs"`

	Limits struct {
		// QueryRate specifies maximum amount of requests per second.
		QueryRate float64 `config:"queryRate" default:"2"`

		// SearchRequests specifies maximum amount of simultaneous requests per second.
		SearchRequests int `config:"searchRequests" default:"32"`
		// BulkRequests specifies maximum amount of simultaneous requests per second.
		BulkRequests int `config:"bulkRequests" default:"32"`
		// InflightBulks specifies maximum amount of simultaneous requests per second.
		InflightBulks int `config:"inflightBulks" default:"32"`

		// FractionHits specifies maximum amount of fractions that can be processed
		// within single search request.
		FractionHits int `config:"fractionHits" default:"6000"`
		// SearchDocs specifies maximum amount of documents that can be returned
		// within single search request.
		SearchDocs int `config:"searchDocs" default:"100000"`
		// DocSize specifies maximum possible size for single document.
		// Document larger than this threshold will be skipped.
		DocSize Bytes `config:"docSize" default:"128KiB"`

		Aggregation struct {
			// FieldTokens specifies maximum amount of unique field tokens
			// that can be processed in single aggregation requests.
			FieldTokens int `config:"fieldTokens" default:"1000000"`
			// FieldTokens specifies maximum amount of unique group tokens
			// that can be processed in single aggregation requests.
			GroupTokens int `config:"groupTokens" default:"2000"`
			// FractionTokens specifies maximum amount of unique tokens
			// that are contained in single fraction which was picked up by aggregation request.
			FractionTokens int `config:"fractionTokens" default:"100000"`
		} `config:"aggregation"`
	} `config:"limits"`

	CircuitBreaker struct {
		Bulk struct {
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			ShardTimeout time.Duration `config:"shardTimeout" default:"10s"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			ErrPercentage int `config:"errPercentage" default:"50"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			BucketWidth time.Duration `config:"bucketWidth" default:"1s"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			BucketsCount int `config:"bucketsCount" default:"10"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			SleepWindow time.Duration `config:"sleepWindow" default:"5s"`
			// Checkout [CircuitBreaker] for more information.
			// [CircuitBreaker]: https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md
			VolumeThreshold int `config:"volumeThreshold" default:"5"`
		} `config:"bulk"`
	} `config:"circuitBreaker"`

	Resources struct {
		// ReaderWorkers specifies number of workers for readers pool.
		// By default this setting is equal to [runtime.GOMAXPROCS].
		ReaderWorkers int `config:"readerWorkers"`
		// SearchWorkers specifies number of workers for searchers pool.
		// By default this setting is equal to [runtime.GOMAXPROCS].
		SearchWorkers int `config:"searchWorkers"`
		// CacheSize specifies maxium size of cache.
		// By default this setting is equal to 30% of available RAM.
		CacheSize         Bytes `config:"cacheSize"`
		SortDocsCacheSize Bytes `config:"sortDocsCacheSize"`
		SkipFsync         bool  `config:"skipFsync"`
	} `config:"resources"`

	Compression struct {
		DocsZstdCompressionLevel     int `config:"docsZstdCompressionLevel" default:"1"`
		MetasZstdCompressionLevel    int `config:"metasZstdCompressionLevel" default:"1"`
		SealedZstdCompressionLevel   int `config:"sealedZstdCompressionLevel" default:"3"`
		DocBlockZstdCompressionLevel int `config:"docBlockZstdCompressionLevel" default:"3"`
	} `config:"compression"`

	Indexing struct {
		MaxTokenSize           int           `config:"maxTokenSize" default:"72"`
		CaseSensitive          bool          `config:"caseSensitive"`
		PartialFieldIndexing   bool          `config:"partialFieldIndexing"`
		AllowedTimeDrift       time.Duration `config:"allowedTimeDrift" default:"24h"`
		FutureAllowedTimeDrift time.Duration `config:"futureAllowedTimeDrift" default:"5m"`
	} `config:"indexing"`

	Mapping struct {
		// Path to mapping file or 'auto' to index all fields as keywords.
		Path string `config:"path"`
		// EnableUpdates will periodically check mapping file and reload configuration if there is an update.
		EnableUpdates bool `config:"enableUpdates"`
		// UpdatePeriod manages how often mapping file will be checked for updates.
		UpdatePeriod time.Duration `config:"updatePeriod" default:"30s"`
	} `config:"mapping"`

	DocsSorting struct {
		Enabled      bool  `config:"enabled"`
		DocBlockSize Bytes `config:"docBlockSize"`
	} `config:"docsSorting"`

	AsyncSearch struct {
		DataDir     string `config:"dataDir"`
		Concurrency int    `config:"concurrency"`
	} `config:"asyncSearch"`

	API struct {
		// EsVersion is the default version that will be returned in the `/` handler.
		ESVersion string `config:"esVersion" default:"8.9.0"`
	} `config:"api"`

	Tracing struct {
		SamplingRate float64 `config:"samplingRate" default:"0.01"`
	} `config:"tracing"`

	/* Non-tweakable parameters */
	MaxFetchSizeBytes Bytes `config:"-" default:"4MB"`
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
