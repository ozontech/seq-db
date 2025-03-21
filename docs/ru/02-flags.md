# Flags

## Flags for Ingestor and Store Modes

### Basic Flags

- **--help:** Show short help
- **--mode="ingestor":** Operation mode. You can choose between `ingestor` and `store` modes.
    - In store mode, seq-db acts as a stateful replica responsible for storing its part of the data. Implements an internal API that is used to communicate between the store and ingestor.
    - In ingestor mode, seq-db acts as a shard and replica coordinator. Implements a public client API. Does not have its own state.
- **--addr=":9002":** Depending on the mode:
    - For ingestor mode, this is the address of the public HTTP API (bulk method). 
    - For store mode, this is the address of the internal gRPC API. By default, port 9002 is used.
- **--debug-addr=":9200":** Address for debugging requests (HTTP). Go metrics and profiling are sent to this address. By default, port 9200 is used.
- **--tracing-probability=0.01:** Tracing probability.

### Indexing Flags

- **--mapping=MAPPING:** Path to the file with indexing parameters or value `auto`. See the corresponding section.
- **--case-sensitive:** Token case sensitivity. By default, if not specified, the search is case-insensitive.
- **--max-token-size=72:** Maximum token size.
- **--partial-indexing:** By default, if the indexed value exceeds the maximum size, it is ignored and does not get into the index. If this parameter is set, the value will be truncated to the maximum length and indexed.

## Flags for Ingestor Mode

- **--proxy-grpc-addr=":9004":** Address for gRPC requests. By default, port 9004 is used.

### Clustering Flags

- **--write-stores=WRITE-STORES:** List of hosts for writing data. Specified as a string with values ​​separated by commas. For example, `--write-stores=host1,host2,host3,host4`.
- **--read-stores=READ-STORES:** List of hosts for reading data. If not set, `--write-stores` is used. Can be used, for example, in case of data migration to other hosts, when we write to some stores and read from other, old stores.
- **--hot-stores=HOT-STORES:** List of `hot` storage hosts. If specified, the proxy works with 2 store clusters: cold (`--write-stores`) and hot (`--hot-stores`). And sends each write request to each of these clusters accordingly. But when reading, it first tries to get data from the `hot` cluster and in some cases from the `cold` one.
- **--hot-read-stores=HOT-READ-STORES:** List of `hot` storage hosts for reading. Can be used when migrating data to other hosts.
- **--store-mode="":** Storage operating mode. If specified, the allowed values ​​are `hot` or `cold`. If the `hot` mode is selected, then when executing a search query, if the requested data range is older than the oldest store documents, the service will return a special error `query wants old data`. Ingestor will make a request to the `cold` store cluster in this case.
- **--replicas=1:** Replication factor for storages. If N is specified, the first N hosts in the list are replicas of one shard, the next N are replicas of another shard, etc.
- **--hot-replicas=HOT-REPLICAS:** Replication factor for hot storages. If not specified, the global factor is used.
- **--shuffle-replicas:** Shuffle replicas before performing a search. If not specified, then the first replica is always read, and only in case of failure, the second one comes.

### Bulk Request Flags

- **--bulk-shard-timeout=10s:** Timeout for processing a bulk operation by one shard.
- **--bulk-err-percentage=50:** Error percentage for triggering the overload protection mechanism. See circuitbreaker/README.md for more details.
- **--bulk-bucket-width=1s:** Window width for counting errors. See circuitbreaker/README.md for more details.
- **--bulk-err-count=10:** Number of errors required to trigger bulk protection mechanism. See circuitbreaker/README.md for more details.
- **--bulk-sleep-window=5s:** Time to wait after bulk protection mechanism is triggered. See circuitbreaker/README.md for more details.
- **--bulk-request-volume-threshold=5:** Request volume threshold for bulk protection mechanism to be triggered. See circuitbreaker/README.md for more details.
- **--max-inflight-bulks=32:** Maximum number of concurrent bulk requests that can be processed.

### Limits Flags

- **--query-rate-limit=2.0:** Maximum request rate per second. `Search` and `fetch` requests are counted. If the limit is exceeded, the request will return an error.

### Compression Flags

- **--docs-zstd-compress-level=3:** ZSTD compression level for documents. More information can be found in the documentation: https://facebook.github.io/zstd/zstd_manual.html.
- **--metas-zstd-compress-level=3:** ZSTD compression level for metadata. More information can be found in the documentation: https://facebook.github.io/zstd/zstd_manual.html.

### Others Ingestor Flags

- **--allowed-time-drift=24h:** Maximum allowed time since the document's timestamp.
- **--future-allowed-time-drift=5m:** Maximum allowed future time since the document's timestamp.
- **--es-version="8.9.0":** Elasticsearch version to return in `/` handler.
- **--mirror-addr="":** Seqproxy mirror address. Used for debugging and profiling with load mirroring.


## Flags For Store Mode

### Data Configuration Flags

- **--data-dir=DATA-DIR:** Directory where data is stored.
- **--frac-size=128MB:** Size of one fraction (minimum fragment of data on disk). The larger the fraction size, the more RAM is required for fraction buffering.
- **--total-size=1GB:** Maximum size of all data. If the data becomes larger than this limit, the oldest data is deleted until the dataset size fits within the specified value.

### Bulk Request Flags

- **--requests-limit=16:** Maximum number of simultaneous bulk requests.
- **--skip-fsync:** Skip fsync operations for the active faction. Speeds up data insertion at the expense of reduced data delivery guarantee.

### Flags Affecting Search Performance

- **--reader-workers=128:** The size of the reader pool from the disk. For SSDs, it is recommended to set it equal to the number of cores.
- **--search-workers-count=128:** The number of worker threads that will process the search by fraction. It is recommended to set it equal to the number of cores.
- **--search-requests-limit=30:** The maximum number of simultaneous search requests. If exceeded, the request will return an error.
- **--search-fraction-limit=6000:** The maximum number of fractions used in the search. If the query requires reading more fractions, the query will return an error.
- **--max-search-docs=100000:** The maximum number of documents returned by the search query.
- **--cache-size=8GB:** The maximum cache size. Used when processing search and fetch requests.

### Compression Flags

- **--seal-zstd-compress-level=3:** ZSTD compression level for sealed data.

### Aggregation Flags

- **--agg-max-group-tokens=2000:** Maximum number of unique tokens for a grouping field affected by an aggregation query. Setting this value to 0 disables the limitation.
- **--agg-max-field-tokens=1000000:** Maximum number of unique tokens for an aggregation function field affected by an aggregation query. Setting this value to 0 disables the limitation.
- **--agg-max-fraction-tids=100000:** Maximum number of unique tokens per fraction for an grouping field or for an aggregation function calculation field. Setting this value to 0 disables the limitation.

### Logging Flags

- **--log-search-threshold-ms=3000:** `Search` query logging threshold in milliseconds. All queries that take longer than the specified time are written to the log.
- **--log-fetch-threshold-ms=3000:** `Fetch` query logging threshold in milliseconds. All queries that take longer than the specified time are written to the log.
- **--log-bulk-threshold-ms=LOG-BULK-THRESHOLD-MS:** `Bulk` query logging threshold in milliseconds. A record of such a query is written to the log.
