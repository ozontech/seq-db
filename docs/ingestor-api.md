# Ingestor Api

## `/_bulk`

Receives body, parses it to docs and metas and write to stores by `grpc bulk` method.
Expects `Content-Type: application/json` in headers.

There should be no more than 32 requests at the same time (`consts.IngestorMaxInflightBulks`), otherwise it returns `429` status code and body

```json
{"took":0,"errors":true,"items":[]}
```

In case of error it returns `500` status code and

```json
{"took":0,"errors":true,"items":[]}
```

Body format `index\ndoc\n` and etc, example:

```bash
curl "localhost:9002/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
{"index":{"_index":"index-other","_type":"span"}}
{"message": "world", "kind": "normal"}
{"index":{"_index":"index-main","_type":"span"}}
{"message": "beautiful", "kind": "old"}
{"index":{"_index":"index-other","_type":"span"}}
{"message": "place", "kind": "old"}
```

Correct answer returns `200` status code and body:

```json
{"took":0,"errors":false,"items":[{"create":{"status":201}},{"create":{"status":201}},{"create":{"status":201}},{"create":{"status":201}}]}
```

## `/fetch`

Returns requested documents by id

Example request:
```bash
curl localhost:9002/fetch -H "Content-Type: application/json" -d \
  '{"ids":["46f538c981010000-a40107d2ef6f20b0", "46f538c981010000-a401f1d250f59393", "087fb5a481010000-6b005e756e060ff8","087fb5a481010000-6b00b5ecbf51ff54","46f538c981010000-a40107d2ef6f20b0", "xxx"]}'
```

Example response:

```json
[
  {
    "_id": "46f538c981010000-a40107d2ef6f20b0",
    "_index": "seq-db",
    "_type": "message",
    "_version": 1,
    "_primary_term": 1,
    "_seq_no": 1,
    "_source": {
      "message": "place",
      "kind": "old"
    },
    "found": true
  },
  {
    "_id": "46f538c981010000-a401f1d250f59393",
    "_index": "seq-db",
    "_type": "message",
    "_version": 1,
    "_primary_term": 1,
    "_seq_no": 1,
    "_source": {
      "message": "beautiful",
      "kind": "old"
    },
    "found": true
  },
  {
    "_id": "087fb5a481010000-6b005e756e060ff8",
    "_index": "seq-db",
    "_type": "message",
    "_version": 1,
    "_primary_term": 1,
    "_seq_no": 1,
    "_source": {
      "message": "beautiful",
      "kind": "old"
    },
    "found": true
  },
  {
    "_id": "087fb5a481010000-6b00b5ecbf51ff54",
    "_index": "seq-db",
    "_type": "message",
    "_version": 1,
    "_primary_term": 1,
    "_seq_no": 1,
    "_source": {
      "message": "place",
      "kind": "old"
    },
    "found": true
  },
  {
    "_id": "46f538c981010000-a40107d2ef6f20b0",
    "_index": "seq-db",
    "_type": "message",
    "_version": 1,
    "_primary_term": 1,
    "_seq_no": 1,
    "_source": {
      "message": "place",
      "kind": "old"
    },
    "found": true
  },
  {
    "_id": "xxx",
    "_index": "seq-db",
    "_type": "message",
    "found": false,
    "error": "wrong id len, should be 33"
  }
]
```

## `/search`

Search and returns docs.
Expects `Content-Type: application/json` in headers.

There are three main types of query:
 - regular search query (see [`search-multi.sh`](../tests/data/search.sh))
 - aggregation query (see [`search-aggregation.sh`](../tests/data/search-aggregation.sh))
 - histogram query (see [`search-histogram.sh`](../tests/data/search-histogram.sh))

Example of regular search request:

```bash
curl "localhost:9002/complex-search" -H 'Content-Type: application/json' -d \
  '{
  "query":{"query":"service:hello", "from":"2024-01-01T15:00:00Z", "to":"2077-12-31T15:00:00Z"},
  "aggs":[{"field":"size_bytes", "group_by":"service", "func":"AGG_FUNC_SUM"}],
  "size": 1000, "offset": 0, "with_total": true,
  "hist": {"interval": "1m"}
  }'
```

Example response:

```json
{
  "total": 10,
  "docs": [
    {
      "id": "1644045f90010000-a60286a369b70895",
      "data": {
        "level": "info",
        "ts": "2024-06-28T15:02:41.396Z",
        "message": "cache stats",
        "total_size": "12.0 GB",
        "overcommitted": "0 B"
      }
    },
    {
      "id": "baed035f90010000-b2019bd4ff8583b2",
      "data": {
        "level": "info",
        "ts": "2024-06-28T15:00:49.449Z",
        "message": "seal block stats",
        "type": "info",
        "raw": "387 B",
        "compressed": "387 B",
        "ratio": 1,
        "blocks_count": 1,
        "write_duration_ms": 0
      }
    },
    {
      "id": "69b1025f90010000-dd03f13039c18e40",
      "data": {
        "level": "info",
        "ts": "2024-06-28T15:00:16.395Z",
        "message": "truncating last fraction",
        "fraction": "sealed fraction name=seq-db-01J16NV1JPPP0AZS3B1ZH2HVR6, creation time=2024-06-25 03:08:13.27, from=2024-06-25 03:07:56.347, to=2024-06-25 03:08:35.903, raw docs=1.3 GB, disk docs=258.8 MB"
      }
    },
    {
      "id": "9508025f90010000-d401dd9a62bcfda5",
      "data": {
        "level": "info",
        "ts": "2024-06-28T14:59:50.396Z",
        "message": "maintenance finished",
        "took_ms": 1
      }
    },
    {
      "id": "28d8015f90010000-ad0175c574130432",
      "data": {
        "level": "info",
        "ts": "2024-06-28T14:59:11.396Z",
        "message": "layer info",
        "name": "lids",
        "size": "8.2 GB"
      }
    },
    {
      "id": "9332fd5e90010000-4403b8513052eff7",
      "data": {
        "level": "info",
        "ts": "2024-06-28T15:03:56.068954586Z",
        "logger": "fd.logd",
        "message": "pipeline stats",
        "stat": "interval=5s, active procs=0/8, events in use=372/144000, out=89547|112.8Mb, rate=17909/s|22.6Mb/s, read ops=0/s, total=26667928625|31666198.5Mb, avg size=1"
      }
    },
    {
      "id": "d02ffd5e90010000-4e00c52c3d11b5ae",
      "data": {
        "level": "info",
        "ts": "2024-06-26T15:20:53.133Z",
        "message": "bulks written",
        "count": 73,
        "docs": 57744,
        "took_ms": 4020,
        "inflight_bulks": 2
      }
    }
  ],
  "aggs": [
    {
      "key": "_not_exists",
      "docCount": "71532765"
    },
    {
      "key": "new item not created yet",
      "docCount": "276"
    },
    {
      "key": "context canceled",
      "docCount": "44"
    },
    {
      "key": "resource not found",
      "docCount": "6"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"082155\"}",
      "docCount": "4"
    },
    {
      "key": "item with offer_id a-cm013-ip15pm-gr already exists for company 2131546",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"010783\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"057920\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"062024\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"075333\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"076937\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"083082\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"085652\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"085655\", \"085654\"}",
      "docCount": "2"
    },
    {
      "key": "item with the same spu (parent variant id) already exists: []string{\"086723\"}",
      "docCount": "2"
    }
  ],
  "hist": {
    "buckets": [
      {
        "doc_count": 4,
        "ts": {
          "seconds": 1719579360
        }
      },
      {
        "doc_count": 121,
        "ts": {
          "seconds": 1719579600
        }
      },
      {
        "doc_count": 76,
        "ts": {
          "seconds": 1719580500
        }
      },
      {
        "doc_count": 32,
        "ts": {
          "seconds": 1719580560
        }
      },
      {
        "doc_count": 25,
        "ts": {
          "seconds": 1719580860
        }
      },
      {
        "doc_count": 11,
        "ts": {
          "seconds": 1719580920
        }
      },
      {
        "doc_count": 22,
        "ts": {
          "seconds": 1719580980
        }
      }
    ]
  },
  "error": {
    "code": 1
  }
}
```

## `/`
Returns a hardcoded ES response that specifies the ElasticSearch version used. This makes Filebeat believe that it has a healthy connection to an ES cluster.
```json
	{
		"cluster_name": "seq-db",
		"version": {
			"number": "6.8.23"
		}
	}
```
