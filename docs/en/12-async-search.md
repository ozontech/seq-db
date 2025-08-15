# Async search

Async searches provide the ability to run search requests in the background.

This ability is especially valuable when there is a need to execute some long-running queries which take longer time to complete.
Usually this queries are aggregations and histograms.
Async searches can be used to search documents too using `with_docs` field in the [`StartAsyncSearch`](10-public-api.md#startasyncsearch) request, though primary use case is aggregations.

Read [API docs](10-public-api.md#async-search-grpc-api) for more info about the public API.

Async searches' data is persisted on disk for the specified `retention` time.
Minimum retention is 5 minutes and maximum is 30 days.
Retention is set by `retention` field in the [`StartAsyncSearch`](10-public-api.md#startasyncsearch) request.
The data is deleted after retention time passes.

When data size exceeds the limits, read only mode is enabled: new async searches are rejected, existing not finished searches are suspended until some disk space is freed by deleting older searches by retention.

## Configuration

Configuration parameters are:

* `data_dir` [string] - specifies directory that contains data for asynchronous searches. By default will be subdirectory in `config.storage.data_dir`.
* `concurrency` [int] - specifies the number of concurrent async search executions.
* `max_total_size` [bytes] - specifies maximum total size of async searches' data per one store.
* `max_size_per_request` [bytes] - specifies maximum total size of a single async search's data per one store.

Configuration parameters are part of `async_search` object in the config file.
