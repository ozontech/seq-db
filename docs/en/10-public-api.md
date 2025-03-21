# Public API

## Introduction

seq-db consists of 2 components - proxy and store:

- seq-db proxy and seq-db store communicate via *internal* [seq-db store gRPC API](https://github.com/ozontech/seq-db/tree/main/api/storeapi).
- To search, clients should use [seq-db proxy gRPC API](https://github.com/ozontech/seq-db/tree/main/api/seqproxyapi/v1).
- To bulk, clients should
  use [HTTP API elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
- To debug search API, we have `seq-db proxy HTTP API`.

This document describes `seq-db proxy gRPC API` in details

## Bulk HTTP API

seq-db compatible with bulk API elasticsearch

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

Example response:

```json
{
  "took": 11,
  "errors": false,
  "items": [
    {
      "create": {
        "status": 201
      }
    },
    {
      "create": {
        "status": 201
      }
    },
    ...
    {
      "create": {
        "status": 201
      }
    }
  ]
}

```

You can notice that service field `index` is left empty. seq-db ignores data passed in this field, since it uses mapping
for field indexing. More information about mapping in [relevant document](03-index-types.md)

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

Method of getting aggregations by aggregation query

Example request:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z"
  },
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_AVG"
    }
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

Example successful response

```json
{
  "aggs": [
    {
      "buckets": [
        {
          "docCount": "12",
          "key": "seq-db",
          "value": 12
        },
        {
          "docCount": "7",
          "key": "seq-proxy",
          "value": 7
        }
      ]
    }
  ],
  "error": {
    "code": "ERROR_CODE_NO"
  }
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