---
id: async-search
---

# Async search

Async searches provide the ability to run search requests in the background.

This ability is especially valuable when executing long-running queries that take longer to complete.
Usually this queries are aggregations and histograms.
Async searches can also be used to search documents using `with_docs` field in the [`StartAsyncSearch`](10-public-api.md#startasyncsearch) request, though primary use case is aggregations.

Read [API docs](10-public-api.md#async-search-grpc-api) for more info about the public API.

Async search data is persisted on disk for the specified `retention` time.
Minimum retention is 5 minutes and maximum is 30 days.
Retention is set by the `retention` field in the [`StartAsyncSearch`](10-public-api.md#startasyncsearch) request.
Data is deleted after the retention period expires.

When data size exceeds the limits, read-only mode is enabled: new async searches are rejected, and unfinished searches are suspended until disk space is freed by deleting older searches by retention.

## Configuration

Configuration parameters are:

* `data_dir` [string] - directory that contains async search data. By default, it is a subdirectory of `config.storage.data_dir`.
* `concurrency` [int] - number of concurrent async search executions.
* `max_total_size` [bytes] - maximum total size of all async searches data per store.
* `max_size_per_request` [bytes] - maximum total size of a single async search data per store.

Configuration parameters are part of `async_search` object in the config file.
