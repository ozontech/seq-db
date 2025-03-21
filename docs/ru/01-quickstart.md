---
id: quickstart
slug: /
---

# Quickstart

Welcome to the seq-db quickstart guide! In just a few minutes, you'll learn how to:

- Quickly spin up a seq-db instance
- Write and store sample log messages
- Query and retrieve messages using search filters

## Running seq-db

### Docker image

seq-db can be quickly launched in a docker container. Pull seq-db image from Docker hub and create a container:

```bash
docker run --rm \
  -p 9002:9002 \ # Default HTTP port
  -p 9004:9004 \ # Default gRPC port
  -p 9200:9200 \ # Default debug port
  -it ghcr.io/ozontech/seq-db:latest --mapping auto --mode single
```

Note that in this example we use a default mapping file (built into the docker image) as seq-db doesn't index any fields by default.
The example uses the `--mode single` flag to run both seq-db in a single binary, rather than in cluster mode and `--mapping auto` to index all fields as `keyword`.

Be aware that we set `--mapping` to `auto` for easier quickstart but this option is not production friendly.
So we encourage you to read more about [mappings and how we index fields](03-index-types.md) and seq-db architecture and operating modes (single/cluster).

## Write documents to seq-db

### Writing documents using `curl`

seq-db supports elasticsearch `bulk` API, so, given a seq-db single instance is listening on port 9002,
a single document can be added like this:

```bash 
curl --request POST \
  --url http://localhost:9002/_bulk \
  --header 'Content-Type: application/json' \
  --data '{"index" : {"unused-key":""}}
{"k8s_pod": "app-backend-123", "k8s_namespace": "production", "k8s_container": "app-backend", "request": "POST", "request_uri": "/api/v1/orders", "message": "New order created successfully"}
{"index" : {"unused-key":""}}
{"k8s_pod": "app-frontend-456", "k8s_namespace": "production", "k8s_container": "app-frontend", "request": "GET", "request_uri": "/api/v1/products", "message": "Product list retrieved"}
{"index" : {"unused-key":""}}
{"k8s_pod": "payment-service-789", "k8s_namespace": "production", "k8s_container": "payment-service", "request": "POST", "request_uri": "/api/v1/payments", "message": "Payment processing failed: insufficient funds"}
'
```

## Search for documents

We'll wrap up this guide with a simple search query
that filters the ingested logs by the `message` field.

Note: make sure `curl` and `jq` are installed to run this example.

```bash
curl --request POST   \
  --url http://localhost:9002/search \
  --header 'Content-Type: application/json' \
  --header 'Grpc-Metadata-use-seq-ql: true' \
  --data-binary @- <<EOF | jq .
  {
    "query":{
      "query":"message: failed",
      "from": "2025-02-11T10:30:00Z",
      "to": "2030-11-25T17:50:30Z"
    },
    "size": 100,
    "offset": 0
  }
EOF
```

## What's next

seq-db offers many more useful features for working with logs. Here's a couple:

- A custom query language - [seq-ql](05-seq-ql.md) - that supports pipes, range queries, wildcards and more.
- Built-in support for various types of aggregations: sum, avg, quantiles etc. TODO add aggregation doc?
- The ability to combine multiple aggregations into a single request using complex-search TODO add link
- Document-ID based retrieval can be [fetched](06-ingestor-api.md#fetch)
