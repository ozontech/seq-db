# Public API

## Introduction

seq-db consists of 2 components - proxy and store:

- seq-db proxy and seq-db store communicate via
  *internal* [seq-db store gRPC API](https://github.com/ozontech/seq-db/tree/main/api/storeapi).
- Querying logs is done via [seq-db proxy gRPC API](https://github.com/ozontech/seq-db/tree/main/api/seqproxyapi/v1).
- Ingesting logs is done via [Elasticsearch compatible HTTP API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
- For debugging purposes, seq-db proxy has `HTTP API` similar to `gRPC API`

This document describes `seq-db ingestion API` and `seq-db search API`

## Bulk HTTP API

seq-db is compatible with Elasticsearch bulk API

### `/`

Returns a hardcoded ES response that specifies the Elasticsearch version used. This feature allows the usage of seq-db
as an Elasticsearch output for logstash, filebeat, and other log shippers.

Example request:

```bash
curl -X GET http://localhost:9002/
```

Example successful response:

```json
{
  "cluster_name": "seq-db",
  "version": {
    "number": "8.9.0"
  }
}
```

### `/_bulk`

Receives body, parses it to docs and metas and writes to stores via internal API.

Example request:

```bash
curl -X POST http://localhost:9002/_bulk -d '
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "5", "time": "2024-12-23T18:00:36.357Z"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "6"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "7"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "8"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "9"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "10"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "11"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "12"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "13"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "14"}
'
```

Example successful response:

```json
{
  "took": 11,
  "errors": false,
  "items": [
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } }
  ]
}
```

One can notice that `index` field is left empty. seq-db ignores data passed in this field, since it uses mapping
for field indexing. You can find more information about mapping in [relevant document](03-index-types.md)

#### Example rate-limited response

There should not be more than `consts.IngestorMaxInflightBulks` requests at the same time (32 by default), otherwise
request is rate-limited and seq-db will response with [`429`](https://developer.mozilla.org/ru/docs/Web/HTTP/Reference/Status/429) status code.

#### Example error response

In case of error seq-db returns `500` status code and the error message.
E.g. if we try to ingest corrupted json

```bash
curl -v -X POST http://localhost:9002/_bulk -d '
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "123}
'
```

We get

```text
processing doc: unexpected end of string near `", "request_time": "123`
                                                  ^
```

## Search gRPC API

### `/Search`

Document search method by request. Takes in query in seq-ql format and returns list of satisfying documents.

Example request:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z",
    "query": "k8s_pod:seq-db"
  },
  "size": 2,
  "with_total": true
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/Search
```

Example successful response:

```json
{
  "total": "5",
  "docs": [
    {
      "id": "0593adf493010000-d901eee224290dc6",
      "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMyJ9",
      "time": "2024-12-23T18:00:36.357Z"
    },
    {
      "id": "0593adf493010000-d9013865e424dba1",
      "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMSJ9",
      "time": "2024-12-23T18:00:36.357Z"
    }
  ],
  "error": {
    "code": "ERROR_CODE_NO"
  }
}
```

`data` field contains original document in base64 format. If we try to decode it

```bash
echo 'eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMyJ9' | base64 -d | jq
```

we get

```json
{
  "k8s_pod": "seq-db",
  "request_time": "13"
}
```

### `/GetAggregation`

Aggregations allow the computation of statistical values (sum, average, maximum, minimum, quantile, uniqueness, count) over document fields
that match the query. Also aggregations support computation of statistical values within different time intervals (also known as timeseries).

Aggregations can be invoked in two ways:

- Through a separate gRPC handler: [`GetAggregation`](#getaggregation)
- Together with search and histograms: [`ComplexSearch`](#complexsearch)

> The examples use the `GetAggregation` API, which has the same query and response structure as `ComplexSearch`.

Supported aggregation functions:

- `AGG_FUNC_SUM` — sum of field values
- `AGG_FUNC_AVG` — average value of the field
- `AGG_FUNC_MIN` — minimum value of the field
- `AGG_FUNC_MAX` — maximum value of the field
- `AGG_FUNC_QUANTILE` — quantile computation for the field
- `AGG_FUNC_UNIQUE` — computation of unique field values (not supported in timeseries)
- `AGG_FUNC_COUNT` — count of documents per group

#### Aggregation Examples

Sample documents:

```json lines
{"service": "svc1", "latency": 100}
{"service": "svc1", "latency": 300}
{"service": "svc2", "latency": 400}
{"service": "svc2", "latency": 200}
{"service": "svc3", "latency": 500}
```

##### Computing SUM, AVG, MIN, MAX

**Request:**

```sh
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "field": "latency",
      "func": "AGG_FUNC_SUM"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_AVG"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MIN"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MAX"
    }
  ]
} | grpcurl -plaintext -d @ localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Response:**

```json
{
  "aggs": [
    {
      "buckets": [
        {"value": 1500}
      ]
    },
    {
      "buckets": [
        {"value": 300}
      ]
    },
    {
      "buckets": [
        {"value": 100}
      ]
    },
    {
      "buckets": [
        {"value": 500}
      ]
    }
  ]
}
```

##### Computing SUM, AVG, MIN, MAX with group_by

**Request:**

```sh
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "field": "latency",
      "func": "AGG_FUNC_SUM",
      "group_by": "service"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_AVG",
      "group_by": "service"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MIN",
      "group_by": "service"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MAX",
      "group_by": "service"
    }
  ]
} | grpcurl -plaintext -d @ localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Response:**

```json
{
  "aggs": [
    {
      "buckets": [
        {"key": "svc2", "value": 600},
        {"key": "svc3", "value": 500},
        {"key": "svc1", "value": 400}
      ]
    },
    {
      "buckets": [
        {"key": "svc3", "value": 500},
        {"key": "svc2", "value": 300},
        {"key": "svc1", "value": 200}
      ]
    },
    {
      "buckets": [
        {"key": "svc1", "value": 100},
        {"key": "svc2", "value": 200},
        {"key": "svc3", "value": 500}
      ]
    },
    {
      "buckets": [
        {"key": "svc3", "value": 500},
        {"key": "svc2", "value": 400},
        {"key": "svc1", "value": 300}
      ]
    }
  ]
}
```

##### QUANTILE

> The QUANTILE aggregation function can also be applied with grouping by a field, using the `group_by` field.

**Request:**

```sh
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "field": "latency",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.5,
        0.9
      ]
    }
  ]
} | grpcurl -plaintext -d @ localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Response:**

```json
{
  "aggs": [
    {
      "buckets": [
        {
          "quantiles": [
            300,
            500
          ],
          "value": 300
        }
      ]
    }
  ]
}
```

##### UNIQUE, COUNT

> For `AGG_FUNC_UNIQUE`, `AGG_FUNC_COUNT`, the `field` is not required.

**Request:**

```sh
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "func": "AGG_FUNC_UNIQUE",
      "group_by": "service"
    },
    {
      "func": "AGG_FUNC_COUNT",
      "group_by": "service"
    }
  ]
} | grpcurl -plaintext -d @ localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Response:**

```json
{
  "aggs": [
    {
      "buckets": [
        {"key": "svc1"},
        {"key": "svc2"},
        {"key": "svc3"}
      ]
    },
    {
      "buckets": [
        {"key": "svc1", "value": 2},
        {"key": "svc2", "value": 2},
        {"key": "svc3", "value": 1}
      ]
    }
  ]
}
```

##### COUNT (with interval)

**Request:**

```sh
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query":"*"
  },
  "aggs": [
    {
      "func": "AGG_FUNC_COUNT",
      "group_by": "service",
      "interval": "30s"
    }
  ]
} | grpcurl -plaintext -d @ localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Response:**

```json
{
  "aggs": [
    {
      "buckets": [
        {
          "key": "svc1",
          "value": 2,
          "ts": "2025-08-17T11:46:00Z"
        },
        {
          "key": "svc2",
          "value": 2,
          "ts": "2025-08-17T11:46:30Z"
        },
        {
          "key": "svc3",
          "value": 2,
          "ts": "2025-08-17T11:47:00Z"
        }
      ]
    }
  ]
}
```

### `/GetHistogram`

Method of getting histograms by query

Example request:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z"
  },
  "hist": {
    "interval": "1ms"
  }
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetHistogram
```

Example successful response

```json
{
  "hist": {
    "buckets": [
      {
        "docCount": "1",
        "ts": "2024-12-23T18:00:36.357Z"
      },
      {
        "docCount": "9",
        "ts": "2024-12-23T18:23:41.349Z"
      }
    ]
  },
  "error": {
    "code": "ERROR_CODE_NO"
  }
}

```

### `/ComplexSearch`

Search request combining fetch of [documents](#search), [aggregations](#getaggregation)
and [histograms](#gethistogram)

Example request:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z",
    "query": "k8s_pod:seq-proxy"
  },
  "with_total": true,
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.2,
        0.8,
        0.95
      ]
    }
  ],
  "hist": {
    "interval": "1ms"
  },
  "order": 0
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/ComplexSearch
```

Example successful response:

```json
{
  "total": "5",
  "aggs": [
    {
      "buckets": [
        {
          "docCount": "6",
          "key": "seq-proxy",
          "value": 6,
          "quantiles": [
            6,
            8,
            9
          ]
        }
      ]
    }
  ],
  "hist": {
    "buckets": [
      {
        "docCount": "1",
        "ts": "2024-12-23T18:00:36.357Z"
      },
      {
        "docCount": "4",
        "ts": "2024-12-23T18:23:41.349Z"
      }
    ]
  },
  "error": {
    "code": "ERROR_CODE_NO"
  }
}
```

### `/Fetch`

Method returning stream of documents by passed seq-id's

Example request:

```bash
grpcurl -plaintext -d '
{
  "ids": [
    "25b5c2f493010000-59024b2ba3fb9630",
    "0593adf493010000-5902ee007dfb6547"
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/Fetch
```

Example successful response:

```json lines
{
  "id": "25b5c2f493010000-59024b2ba3fb9630",
  "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMCJ9",
  "time": "2024-12-23T18:23:41.349Z"
}
{
  "id": "0593adf493010000-5902ee007dfb6547",
  "data": "eyJrOHNfcG9kIjoic2VxLXByb3h5IiwgInJlcXVlc3RfdGltZSI6ICI1IiwgInRpbWUiOiAiMjAyNC0xMi0yM1QxODowMDozNi4zNTdaIn0=",
  "time": "2024-12-23T18:00:36.357Z"
}
```

#### `/Mapping`

Method returning mapping seq-db working with

Example request:

```bash
grpcurl -plaintext localhost:9004 seqproxyapi.v1.SeqProxyApi/Mapping
```

Example successful response:

```json
{
  "data": "eyJrOHNfY29udGFpbmVyIjoia2V5d29yZCIsIms4c19uYW1lc3BhY2UiOiJrZXl3b3JkIiwiazhzX3BvZCI6ImtleXdvcmQiLCJtZXNzYWdlIjoidGV4dCIsIm1lc3NhZ2Uua2V5d29yZCI6ImtleXdvcmQiLCJyZXF1ZXN0IjoidGV4dCIsInJlcXVlc3RfdGltZSI6ImtleXdvcmQiLCJyZXF1ZXN0X3VyaSI6InBhdGgiLCJzb21lb2JqIjoib2JqZWN0Iiwic29tZW9iai5uZXN0ZWQiOiJrZXl3b3JkIiwic29tZW9iai5uZXN0ZWR0ZXh0IjoidGV4dCJ9"
}
```

decoding base64 results in:

```json
{
  "k8s_container": "keyword",
  "k8s_namespace": "keyword",
  "k8s_pod": "keyword",
  "message": "text",
  "message.keyword": "keyword",
  "request": "text",
  "request_time": "keyword",
  "request_uri": "path",
  "someobj": "object",
  "someobj.nested": "keyword",
  "someobj.nestedtext": "text"
}
```

#### `/Status`

Method returning detailed information about seq-db stores seq-db proxy working with

Example request:

```bash
grpcurl -plaintext localhost:9004 seqproxyapi.v1.SeqProxyApi/Status
```

Example successful response

```json
{
  "numberOfStores": 1,
  "oldestStorageTime": "2024-12-23T18:23:37.622Z",
  "stores": [
    {
      "host": "localhost:9234",
      "values": {
        "oldestTime": "2024-12-23T18:23:37.622Z"
      }
    }
  ]
}
```

#### `/Export`

Same method as a [`/Search`](#search), but streaming

Example request:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z",
    "query": "k8s_pod:seq-db"
  },
  "size": 2,
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/Export
```

Example successful response:

```json lines
{
  "doc": {
    "id": "25b5c2f493010000-5902919c44e568be",
    "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMyJ9",
    "time": "2024-12-23T18:23:41.349Z"
  }
}
{
  "doc": {
    "id": "25b5c2f493010000-5902d3ff804c179d",
    "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMiJ9",
    "time": "2024-12-23T18:23:41.349Z"
  }
}
```

## Async search gRPC API

### `/StartAsyncSearch`

Start an async search.

Returns an ID that can later be used to fetch results, cancel, or delete the search.

Similar to [`/ComplexSearch`](#complexsearch), but with additional fields: `retention` and `with_docs`.
`retention` determines how long the data will be available.
`with_docs` determines whether document IDs that match the query are found and saved.
If `with_docs` is true, `with_total` is automatically set to true.

Example request:

```bash
grpcurl -plaintext -d '
{
  "retention": "3600s",
  "query": {
    "from": "2025-07-01T05:20:00Z",
    "to": "2025-09-01T05:21:00Z",
    "query": "message:error | fields level, message"
  },
  "hist": {
    "interval": "1ms"
  },
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.2,
        0.8,
        0.95
      ]
    }
  ],
  "with_docs": true
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/StartAsyncSearch
```

Example successful response:

```json
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10"
}
```

### `/FetchAsyncSearchResult`

Fetch async search result.

Returns execution result based on a search parameters from [`/StartAsyncSearch`](#startasyncsearch) request.
A partial response can be returned if the search is not yet complete. The `status` field indicates the current status.
Available statuses: `AsyncSearchStatusInProgress`, `AsyncSearchStatusDone`, `AsyncSearchStatusCanceled`, `AsyncSearchStatusError`.

Example request:

```bash
grpcurl -plaintext -d '
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10",
  "size": 2,
  "offset": 0,
  "order": 0
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/FetchAsyncSearchResult
```

Example successful response:

```json
{
 "status": "AsyncSearchStatusDone",
 "request": {
  "retention": "3600s",
  "query": {
   "query": "message:peggy | fields level, message",
   "from": "2025-08-01T05:20:00Z",
   "to": "2025-09-01T05:21:00Z"
  },
  "hist": {
    "interval": "1ms"
  },
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.2,
        0.8,
        0.95
      ]
    }
  ],
  "with_docs": true
 },
 "response": {
  "total": 12,
  "docs": [
   {
    "id": "c09e878998010000-9102a3cb83f156f2",
    "data": {
     "message": "some error",
     "level": 3
    },
    "time": "2025-08-08T11:53:43.36Z"
   },
   {
    "id": "c09e878998010000-91026cf9cf3386e3",
    "data": {
     "message": "error 2",
     "level": 3
    },
    "time": "2025-08-08T11:53:43.36Z"
   }
  ],
  "aggs": [
    {
      "buckets": [
        {
          "docCount": "6",
          "key": "seq-proxy",
          "value": 6,
          "quantiles": [
            6,
            8,
            9
          ]
        }
      ]
    }
  ],
  "hist": {
    "buckets": [
      {
        "docCount": "1",
        "ts": "2024-12-23T18:00:36.357Z"
      },
      {
        "docCount": "4",
        "ts": "2024-12-23T18:23:41.349Z"
      }
    ]
  }
 },
 "progress": 1,
 "disk_usage": "537",
 "started_at": "2025-08-08T11:53:52.542336Z",
 "expires_at": "2025-08-08T12:53:52.542336Z"
}
```

> `response.docs` and `response.total` fields will be saved and returned only if `with_docs` is true in the [`/StartAsyncSearch`](#startasyncsearch) request

### `/GetAsyncSearchesList`

Get async searches list.

Returns list of available async searches filtered out based on fields from request.
Empty request `{}` returns all available at the moment searches.

Example request:

```bash
grpcurl -plaintext -d '
{
  "status": null,
  "size": 2,
  "offset": 0,
  "ids": ["c28c97d1-117a-45dc-a0cf-d080f22b2a10", "8e68d3d7-8e85-44a9-9b09-8de248a3b414"]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAsyncSearchesList
```

Example successful response:

```json
{
 "searches": [
  {
   "searchId": "c28c97d1-117a-45dc-a0cf-d080f22b2a10",
   "status": "AsyncSearchStatusInProgress",
   "request": {
    "retention": "3600s",
    "query": {
     "query": "message:error | fields level, message",
     "from": "2025-07-01T05:20:00Z",
     "to": "2025-08-01T05:21:00Z"
    },
    "aggs": [],
    "with_docs": true
   },
   "startedAt": "2025-08-08T11:49:20.707200Z",
   "expiresAt": "2025-08-08T12:49:20.707200Z",
   "progress": 0.89,
   "diskUsage": "282"
  },
  {
   "searchId": "8e68d3d7-8e85-44a9-9b09-8de248a3b414",
   "status": "AsyncSearchStatusDone",
   "request": {
    "retention": "3600s",
    "query": {
     "query": "message:error",
     "from": "2025-07-01T05:20:00Z",
     "to": "2025-08-01T05:21:00Z"
    },
    "aggs": [],
    "with_docs": false
   },
   "startedAt": "2025-08-08T11:48:49.488244Z",
   "expiresAt": "2025-08-08T12:48:49.488244Z",
   "progress": 1,
   "diskUsage": "283"
  }
 ]
}
```

### `/CancelAsyncSearch`

Cancel async search.

Cancels async search. Canceled search's execution will be stopped. Can't cancel finished request.

Example request:

```bash
grpcurl -plaintext -d '
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10"
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/CancelAsyncSearch
```

Example successful response:

```json
{}
```

Empty response is successful.

### `/DeleteAsyncSearch`

Delete async search.

Marks async search as expired, the data will be removed the next maintenance iteration.

Example request:

```bash
grpcurl -plaintext -d '
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10"
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/DeleteAsyncSearch
```

Example successful response:

```json
{}
```

Empty response is successful.
