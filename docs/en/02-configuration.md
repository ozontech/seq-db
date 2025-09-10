---
id: configuration
---

# Configuration

This document describes all available configuration options for the system.

## Address Configuration

Network addresses for various services.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `address.http` | string | `:9002` | HTTP listen address |
| `address.grpc` | string | `:9004` | GRPC listen address |
| `address.debug` | string | `:9200` | Debug listen address |

## Storage Configuration

Storage settings for data persistence.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage.data_dir` | string | - | Path to a directory where fractions will be stored |
| `storage.frac_size` | Bytes | `128MiB` | Maximum size of an active fraction before it gets sealed |
| `storage.total_size` | Bytes | `1GiB` | Upper bound of how much disk space can be occupied by sealed fractions before they get deleted (or offloaded) |

## Cluster Configuration

Cluster topology and replication settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cluster.write_stores` | []string | - | Cold store instances which will be written to |
| `cluster.read_stores` | []string | - | Cold store instances wich will be queried from |
| `cluster.hot_stores` | []string | - | Store instances which will be written to and queried from |
| `cluster.hot_read_stores` | []string | - | Store instances which will be queried from. This field is optional but if specified will take precedence over `cluster.hot_stores` |
| `cluster.replicas` | int | `1` | Number of instances that belong to one shard |
| `cluster.hot_replicas` | int | - | Number if hot instances that belong to one shard. If specified will take precedence over `cluster.replicas` for hot stores |
| `cluster.shuffle_replicas` | bool | `false` | Whether to shuffle replicas |
| `cluster.mirror_address` | string | - | Host to which search queries will be mirrored. It can be useful if you have development cluster and you want to have same search pattern as you have on production cluster |

## Slow Logs Configuration

Thresholds for logging slow operations.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `slow_logs.bulk_threshold` | Duration | `0ms` | Duration to determine slow bulks. When bulk request exceeds this threshold it will be logged |
| `slow_logs.search_threshold` | Duration | `3s` | Duration to determine slow searches. When search request exceeds this threshold it will be logged |
| `slow_logs.fetch_threshold` | Duration | `3s` | Duration to determine slow fetches. When fetch request exceeds this threshold it will be logged |

## Limits Configuration

Rate limiting and resource constraints.

### General Limits

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `limits.query_rate` | float64 | `2` | Maximum amount of requests per second |
| `limits.search_requests` | int | `32` | Maximum amount of simultaneous requests per second |
| `limits.bulk_requests` | int | `32` | Maximum amount of simultaneous requests per second |
| `limits.inflight_bulks` | int | `32` | Maximum amount of simultaneous requests per second |
| `limits.fraction_hits` | int | `6000` | Maximum amount of fractions that can be processed within single search request |
| `limits.search_docs` | int | `100000` | Maximum amount of documents that can be returned within single search request |
| `limits.doc_size` | Bytes | `128KiB` | Maximum possible size for single document. Document larger than this threshold will be skipped |

### Aggregation Limits

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `limits.aggregation.field_tokens` | int | `1000000` | Maximum amount of unique field tokens that can be processed in single aggregation requests. Setting this field to 0 disables limit |
| `limits.aggregation.group_tokens` | int | `2000` | Maximum amount of unique group tokens that can be processed in single aggregation requests. Setting this field to 0 disables limit |
| `limits.aggregation.fraction_tokens` | int | `100000` | Maximum amount of unique tokens that are contained in single fraction which was picked up by aggregation request. Setting this field to 0 disables limit |

## Circuit Breaker Configuration

Circuit breaker settings for bulk operations. See [CircuitBreaker documentation](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `circuit_breaker.bulk.shard_timeout` | Duration | `10s` | Checkout [CircuitBreaker](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information |
| `circuit_breaker.bulk.err_percentage` | int | `50` | Checkout [CircuitBreaker](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information |
| `circuit_breaker.bulk.bucket_width` | Duration | `1s` | Checkout [CircuitBreaker](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information |
| `circuit_breaker.bulk.buckets_count` | int | `10` | Checkout [CircuitBreaker](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information |
| `circuit_breaker.bulk.sleep_window` | Duration | `5s` | Checkout [CircuitBreaker](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information |
| `circuit_breaker.bulk.volume_threshold` | int | `5` | Checkout [CircuitBreaker](https://github.com/ozontech/seq-db/blob/main/network/circuitbreaker/README.md) for more information |

## Resources Configuration

Resource allocation settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `resources.reader_workers` | int | runtime.GOMAXPROCS | Number of workers for readers pool. By default this setting is equal to runtime.GOMAXPROCS |
| `resources.search_workers` | int | runtime.GOMAXPROCS | Number of workers for searchers pool. By default this setting is equal to runtime.GOMAXPROCS |
| `resources.cache_size` | Bytes | 30% of available RAM | Maxium size of cache. By default this setting is equal to 30% of available RAM |
| `resources.sort_docs_cache_size` | Bytes | - | Size of the sorted documents cache |
| `resources.skip_fsync` | bool | `false` | Whether to skip fsync operations |

## Compression Configuration

Compression level settings for various data types.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression.docs_zstd_compression_level` | int | `1` | Zstandard compression level for documents |
| `compression.metas_zstd_compression_level` | int | `1` | Zstandard compression level for metadata |
| `compression.sealed_zstd_compression_level` | int | `3` | Zstandard compression level for sealed fractions |
| `compression.doc_block_zstd_compression_level` | int | `3` | Zstandard compression level for document blocks |

## Indexing Configuration

Settings for document indexing behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `indexing.max_token_size` | int | `72` | Maximum token size |
| `indexing.case_sensitive` | bool | `false` | Whether indexing is case sensitive |
| `indexing.partial_field_indexing` | bool | `false` | Whether to enable partial field indexing |
| `indexing.past_allowed_time_drift` | Duration | `24h` | How much time can elapse since the message's timestamp. If more time than this has passed since the message's timestamp, the message's timestamp gets overwritten |
| `indexing.future_allowed_time_drift` | Duration | `5m` | Maximum allowable offset for a message's timestamp into the future. If a message's timestamp is further in the future than this, it is overwritten |

## Mapping Configuration

Field mapping configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mapping.path` | string | - | Path to mapping file or 'auto' to index all fields as keywords |
| `mapping.enable_updates` | bool | `false` | Will periodically check mapping file and reload configuration if there is an update |
| `mapping.update_period` | Duration | `30s` | Manages how often mapping file will be checked for updates |

## Documents Sorting Configuration

Settings for document sorting functionality.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `docs_sorting.enabled` | bool | `false` | Enables/disables documents sorting |
| `docs_sorting.doc_block_size` | Bytes | - | Sets document block size. Large size consumes more RAM but improves compression ratio |

## Async Search Configuration

Configuration for asynchronous search operations.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `async_search.data_dir` | string | subdirectory in storage.data_dir | Directory that contains data for asynchronous searches. By default will be subdirectory in `storage.data_dir` |
| `async_search.concurrency` | int | - | Concurrency level for async searches |
| `async_search.max_total_size` | Bytes | `1GiB` | - |
| `async_search.max_size_per_request` | Bytes | `100MiB` | - |

## API Configuration

API-related settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `api.es_version` | string | `8.9.0` | Default version that will be returned in the `/` handler |

## Tracing Configuration

Distributed tracing settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tracing.sampling_rate` | float64 | `0.01` | Sampling rate for distributed tracing |

## Notes

- **Bytes**: Size values can be specified with units like `KiB`, `MiB`, `GiB` (e.g., `128MiB`)
- **Duration**: Time values can be specified with units like `ms`, `s`, `m`, `h` (e.g., `3s`, `24h`)
- **Default Values**: Fields without explicit defaults are required unless marked as optional
- **Arrays**: Fields of type `[]string` accept multiple values
