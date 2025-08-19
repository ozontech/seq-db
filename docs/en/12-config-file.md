# Configuration Reference

This document describes all available configuration options for the application. All configuration flags must be stored within the appropriate YAML key structure as shown in the examples.

## Address Configuration
Config key: `address`

Controls the network addresses for different services.

**address.http** (string, default: `:9002`)  
HTTP listen address for API endpoints.

**address.grpc** (string, default: `:9004`)  
GRPC listen address for GRPC API endpoints.

**address.debug** (string, default: `:9200`)  
Debug listen address for debug/metrics endpoints.

```yaml
address:
  http: ":8080"
  grpc: ":8081"
  debug: ":8082"
```

## Storage Configuration
Config key: `storage`

Manages settings for fractions stored on disk.

**storage.data_dir** (string, required)  
Path to directory where fractions will be stored.

**storage.frac_size** (Bytes, default: `128MiB`)  
Maximum size of an active fraction before it gets sealed.

**storage.total_size** (Bytes, default: `1GiB`)  
Upper bound of disk space for sealed fractions before deletion or offload.


```yaml
storage:
  data_dir: "/data/fractions"
  frac_size: "256MiB"
  total_size: "10GiB"
```

## Cluster Configuration
Config key: `cluster`

Defines cluster topology and replication settings.

**cluster.write_stores** ([]string)  
Addresses of write-only cold store instances.

**cluster.read_stores** ([]string)  
Addresses of read-only cold store instances which will be queried from.

**cluster.replicas** (int, default: `1`)  
Number of instances that belong to one shard.
The replicas in `hot_stores`, `hot_read_stores`, `write_stores`, and `read_stores` are specified
as lists of strings. 
This setting groups the configured stores into shards,
with each shard containing `cluster.hot_replicas` replicas in the order 
specified in the configuration file.

Examples:

Example 1: Single Replica per Shard 
```yaml
cluster:
    hot_replicas: 1
    hot_stores:
      - seq-db-store-1 # shard 1
      - seq-db-store-2 # shard 2
```
In this configuration, each store forms its own shard with a single replica, and load is balanced across both shards.


Example 2: Multiple Replicas per Shard
```yaml
cluster:
    hot_replicas: 2
    hot_stores:
      - seq-db-store-1 # shard 1, replica 1
      - seq-db-store-2 # shard 1, replica 2
      - seq-db-store-3 # shard 2, replica 1
      - seq-db-store-4 # shard 2, replica 2
```
Here, stores are grouped into shards of 2 replicas each, creating 2 shards with 2 replicas per shard.

**

**cluster.hot_stores** ([]string)  
Addresses of hot store instances which will be written to and queried from.

**cluster.hot_read_stores** ([]string)  
Addresses of read-only hot store instances which will be queried from.
This field is optional but if specified will take precedence over hot_stores for read operations.

**cluster.hot_replicas** (int)  
Number of hot instances that belong to one shard. If specified will take precedence over replicas for hot stores.
**cluster.shuffle_replicas** (bool, default: `false`)  
Whether to shuffle replica selection.

**cluster.mirror_address** (string)  
Host to which search queries will be mirrored. It can be useful if you have development cluster and you want to have same search pattern as you have on production cluster.

Example: 
```yaml
cluster:
  replicas: 1
  hot_replicas: 2
  hot_stores:
    - "hot-1:9004" # shard1, replica1
    - "hot-2:9004" # shard1, replica2
  write_stores: 
    - "cold-1:9004" # shard1
    - "cold-2:9004" # shard2
  read_stores:
    - "cold-1:9004" # shard1
    - "cold-2:9004" # shard2
    - "cold-3:9004" # shard2 (read-only)
```

## Slow Logs Configuration

Config key: `slow_logs`

Configures thresholds for logging slow operations.

**slow_logs.bulk_threshold** (duration, default: `0ms`)  
Duration threshold to determine slow bulks. When bulk request exceeds this threshold it will be logged.
Disabled if the value is 0;

**slow_logs.search_threshold** (duration, default: `3s`)  
Duration threshold to determine slow searches. When search request exceeds this threshold it will be logged.

**slow_logs.fetch_threshold** (duration, default: `3s`)  
Duration threshold to determine slow fetches. When fetch request exceeds this threshold it will be logged.

Example: 
```yaml
slow_logs:
  bulk_threshold: "100ms"
  search_threshold: "5s"
  fetch_threshold: "2s"
```

## Limits Configuration
Config key: `limits`

Sets various operational limits and rate limiting.

**limits.query_rate** (float64, default: `2`)  
Maximum amount of requests per second.

**limits.search_requests** (int, default: `32`)  
Maximum amount of simultaneous search requests per second.

**limits.bulk_requests** (int, default: `32`)  
Maximum amount of simultaneous bulk requests per second.

**limits.inflight_bulks** (int, default: `32`)  
Maximum amount of simultaneous inflight bulk requests per second.

**limits.fraction_hits** (int, default: `6000`)  
Maximum amount of fractions that can be processed within single search request.

**limits.search_docs** (int, default: `100000`)  
Maximum amount of documents that can be returned within single search request.

**limits.doc_size** (Bytes, default: `128KiB`)  
Maximum possible size for single document. Documents larger than this threshold will be skipped.

### Aggregation Limits

**limits.aggregation.field_tokens** (int, default: `1000000`)  
Maximum amount of unique field tokens that can be processed in single aggregation request. Setting this field to 0 disables limit.

**limits.aggregation.group_tokens** (int, default: `2000`)  
Maximum amount of unique group tokens that can be processed in single aggregation request. Setting this field to 0 disables limit.

**limits.aggregation.fraction_tokens** (int, default: `100000`)  
Maximum amount of unique tokens that are contained in single fraction which was picked up by aggregation request. Setting this field to 0 disables limit.

Example:
```yaml
limits:
  query_rate: 10.0
  search_requests: 64
  bulk_requests: 64
  fraction_hits: 10000
  search_docs: 50000
  doc_size: "256KiB"
  aggregation:
    field_tokens: 2000000
    group_tokens: 5000
    fraction_tokens: 200000
```

## Circuit Breaker Configuration
Config key: `circuit_breaker`.
Configures circuit breaker behavior for bulk operations. For detailed information about circuit breaker patterns, see the [CircuitBreaker documentation](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md).

**circuit_breaker.bulk.shard_timeout** (duration, default: `10s`)  
Timeout for shard operations.

**circuit_breaker.bulk.err_percentage** (int, default: `50`)  
Error percentage threshold to trigger circuit breaker.

**circuit_breaker.bulk.bucket_width** (duration, default: `1s`)  
Width of each bucket for error tracking.

**circuit_breaker.bulk.buckets_count** (int, default: `10`)  
Number of buckets to maintain for error tracking.

**circuit_breaker.bulk.sleep_window** (duration, default: `5s`)  
Sleep duration when circuit breaker is open.

**circuit_breaker.bulk.volume_threshold** (int, default: `5`)  
Minimum request volume required to trip circuit breaker.

Example:
```yaml
circuit_breaker:
  bulk:
    shard_timeout: "15s"
    err_percentage: 40
    bucket_width: "2s"
    buckets_count: 15
    sleep_window: "10s"
    volume_threshold: 10
```

## Resources Configuration
Config key:`resources`
Controls resource allocation and performance settings.

**resources.reader_workers** (int, default: runtime.GOMAXPROCS)  
Number of workers for readers pool.

**resources.search_workers** (int, default: runtime.GOMAXPROCS)  
Number of workers for searchers pool.

**resources.cache_size** (Bytes, default: 30% of available RAM)  
Maximum size of cache.

**resources.sort_docs_cache_size** (Bytes)  
Cache size for document sorting operations.

**resources.skip_fsync** (bool, default: `false`)  
Skip filesystem sync operations for better performance but reduced durability.

Example:
```yaml
resources:
  reader_workers: 8
  search_workers: 16
  cache_size: "4GiB"
  sort_docs_cache_size: "512MiB"
  skip_fsync: false
```

## Compression Configuration
Config key: `compression`
Controls compression levels for different data types using ZSTD algorithm.

**compression.docs_zstd_compression_level** (int, default: `1`)  
ZSTD compression level for documents.

**compression.metas_zstd_compression_level** (int, default: `1`)  
ZSTD compression level for metadata.

**compression.sealed_zstd_compression_level** (int, default: `3`)  
ZSTD compression level for sealed fractions.

**compression.doc_block_zstd_compression_level** (int, default: `3`)  
ZSTD compression level for document blocks.

```yaml
compression:
  docs_zstd_compression_level: 2
  metas_zstd_compression_level: 1
  sealed_zstd_compression_level: 5
  doc_block_zstd_compression_level: 4
```

## Indexing Configuration
Config key: `indexing`
Controls text indexing and document processing behavior.

**indexing.max_token_size** (int, default: `72`)  
Maximum size of indexed tokens in bytes.

**indexing.case_sensitive** (bool, default: `false`)  
Whether text indexing should be case-sensitive.

**indexing.partial_field_indexing** (bool, default: `false`)  
Enable partial field indexing for better search performance.

**indexing.past_allowed_time_drift** (duration, default: `24h`)  
How much time can elapse since the message's timestamp. If more time than this has passed since the message's timestamp, the message's timestamp gets overwritten.

**indexing.future_allowed_time_drift** (duration, default: `5m`)  
Maximum allowable offset for a message's timestamp into the future. If a message's timestamp is further in the future than this drift, it is overwritten.

```yaml
indexing:
  max_token_size: 128
  case_sensitive: true
  partial_field_indexing: true
  past_allowed_time_drift: "48h"
  future_allowed_time_drift: "10m"
```

## Mapping Configuration
Config key: `mapping`
Controls field mapping and schema management.

**mapping.path** (string)  
Path to mapping file or 'auto' to index all fields as keywords.

**mapping.enable_updates** (bool, default: `false`)  
Periodically check mapping file and reload configuration if there is an update.

**mapping.update_period** (duration, default: `30s`)  
How often mapping file will be checked for updates.

```yaml
mapping:
  path: "/etc/mappings.json"
  enable_updates: true
  update_period: "60s"
```

## Document Sorting Configuration
Config key: `docs_sorting`
Controls document sorting behavior for improved compression ratios.

**docs_sorting.enabled** (bool, default: `false`)  
Enable or disable documents sorting.

**docs_sorting.doc_block_size** (Bytes)  
Document block size for sorting. Large size consumes more RAM but improves compression ratio.

```yaml
docs_sorting:
  enabled: true
  doc_block_size: "8MiB"
```


## Offloading Configuration
Config key: `offloading`  
Controls S3-based offloading of old data fractions to reduce local storage usage.

**offloading.enabled** (bool, default: `false`)  
Enable data offloading to S3.

**offloading.retention** (duration)  
TTL for remote fractions. By default no retention is configured and all remote fractions are kept forever.

**offloading.endpoint** (string, default: `http://s3.us-east-1.amazonaws.com/`)  
S3 endpoint for S3 client.

**offloading.bucket** (string)  
Name of S3 bucket where remote fractions will be stored.

**offloading.region** (string, default: `us-east-1`)  
AWS region for the S3 bucket.

**offloading.access_key** (string)  
S3 Access Key for S3 client authentication. Learn more about access keys in the [AWS documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).

**offloading.secret_key** (string)  
S3 Secret Key for S3 client authentication. Learn more about secret keys in the [AWS documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).

## Asynchronous Search Configuration
Config key: `async_search`  
Controls asynchronous search functionality and resource allocation.

**async_search.data_dir** (string)  
Directory that contains data for asynchronous searches. By default will be a subdirectory in the main storage data directory.

**async_search.concurrency** (int)  
Number of concurrent asynchronous search operations allowed.

## API Configuration
Config key: `api`  
Controls API behavior and compatibility settings.

**api.es_version** (string, default: `8.9.0`)  
The Elasticsearch version that will be returned in the `/` handler for compatibility with some log collectors.

## Tracing Configuration
Config key: `tracing`  
Controls distributed tracing and sampling behavior.

**tracing.sampling_rate** (float64, default: `0.01`)  
Sampling rate for distributed tracing (0.0 to 1.0, where 1.0 means 100% sampling).