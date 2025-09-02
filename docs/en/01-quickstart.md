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

### Single node mode

Before launch you need to create config file:

config.yaml:

```yaml
storage:
  data_dir: /seq-db-data

mapping:
  path: auto
```

seq-db can be quickly launched in a docker container. Pull seq-db image from Docker hub and create a container:

```bash
docker run --rm \
  -p 9002:9002 \
  -p 9004:9004 \
  -p 9200:9200 \
  -v "$(pwd)"/config.yaml:/seq-db/config.yaml \
  -it ghcr.io/ozontech/seq-db:latest --mode single --config=config.yaml
```

Note that in this example we use a default mapping file (built into the docker image) as seq-db doesn't index any fields by default.
The example uses the `--mode single` flag to run both seq-db in a single binary, rather than in cluster mode.
Field indexing is configured via `mapping.path: auto` in config.yaml to index all fields as `keyword`.

### Cluster mode

seq-db can be launched in a cluster mode, two main components are `seq-db-proxy` and `seq-db-store`: `seq-db-proxy` proxies requests to configured stores and `seq-db-store` actually stores the data.

Use `--mode` flag to select specific mode: `proxy` or `store`.

Before launch you need to create config files:

config.yaml:

```yaml
cluster:
  hot_stores:
    - seq-db-store:9004

mapping:
  path: auto
```

Here is the minimal docker compose example:

```yaml
services:
  
  seq-db-proxy:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    ports:
      - "9002:9002" # Default HTTP port
      - "9004:9004" # Default gRPC port
      - "9200:9200" # Default debug port
    command: --mode proxy --config=config.yaml
    depends_on:
      - seq-db-store
  
  seq-db-store:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    command: --mode store --config=config.yaml
```

Read [clustering flags](02-flags.md#clustering-flags) and [long term store](07-long-term-store.md) for more info about possible configurations.

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
{"k8s_pod": "payment-service-789", "k8s_namespace": "production", "k8s_container": "payment-service", "request": "POST", "request_uri": "/api/v1/payments", "message": "failed"}
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
  --data-binary '
  {
    "query":{
      "query":"message:failed",
      "from": "2025-02-11T10:30:00Z",
      "to": "2030-11-25T17:50:30Z"
    },
    "size": 100,
    "offset": 0
  }'
```

## Running seq-db with seq-ui server

seq-ui is a backend for user interface.

To launch seq-db with seq-ui you need to add minimal configuration file for seq-ui, config.seq-ui.yaml:

```yaml
server:
  http_addr: "0.0.0.0:5555"
  grpc_addr: "0.0.0.0:5556"
  debug_addr: "0.0.0.0:5557"
clients:
  seq_db_addrs:
    - "seq-db-proxy:9004"
  seq_db_timeout: 15s
  seq_db_avg_doc_size: 100
  request_retries: 3
  proxy_client_mode: "grpc"
  grpc_keepalive_params:
    time: 10s
    timeout: 10s
    permit_without_stream: true
handlers:
  seq_api:
    seq_cli_max_search_limit: 10000
    max_search_limit: 1000
    max_search_total_limit: 100000
    max_search_offset_limit: 100000
    max_export_limit: 10000
    max_parallel_export_requests: 1
    max_aggregations_per_request: 3
```

Don't forget to include seq-db configuration file, config.yaml:

```yaml
cluster:
  hot_stores:
    - seq-db-store:9004

mapping:
  path: auto
```

And then launch using this minimal docker compose example:

```yaml
services:

  seq-ui:
    image: ghcr.io/ozontech/seq-ui:latest
    volumes:
      - ${PWD}/config.seq-ui.yaml:/seq-ui-server/config.yaml
    ports:
      - "5555:5555" # Default HTTP port
      - "5556:5556" # Default gRPC port
      - "5557:5557" # Default debug port
    command: --config config.yaml
  
  seq-db-proxy:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    ports:
      - "9002:9002" # Default HTTP port
      - "9004:9004" # Default gRPC port
      - "9200:9200" # Default debug port
    command: --mode proxy --config=config.yaml
    depends_on:
      - seq-db-store
  
  seq-db-store:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    command: --mode store --config=config.yaml
```

See seq-ui documentation for more details on how to interact with seq-ui.

## What's next

seq-db offers many more useful features for working with logs. Here's a couple:

- A custom query language - [seq-ql](05-seq-ql.md) - that supports pipes, range queries, wildcards and more.
- Built-in support for various types of aggregations: sum, avg, quantiles etc. TODO add aggregation doc?
- The ability to combine multiple aggregations into a single request using complex-search TODO add link
- Document-ID based retrieval can be [fetched](10-public-api.md#fetch)
