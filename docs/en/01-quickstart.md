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

seq-db can be quickly launched in a docker container.

Before launch you need to create a config file:

config.yaml:

```yaml
mapping:
  path: auto
```

Pull seq-db image from GitHub Container Registry (GHCR) and create a container:

```bash
docker run --rm \
  -p 9002:9002 \
  -p 9004:9004 \
  -p 9200:9200 \
  -v "$(pwd)"/config.yaml:/seq-db/config.yaml \
  -it ghcr.io/ozontech/seq-db:latest --mode single --config=config.yaml
```

**Note:** The `--mode single` flag runs seq-db as a single binary, rather than in cluster mode. And the `mapping.path: auto` setting automatically indexes all fields as `keyword` type.

For more information about supported index types, see [Index Types](./03-index-types.md).

### Cluster mode

Seq-db can be deployed in cluster mode using two main components:

- seq-db-proxy - Routes and proxies requests to configured storage nodes
- seq-db-store - Handles actual data storage and retrieval

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

Alternatively, you can run `docker-compose.yaml` file from `quickstart` directory.

Read [cluster configuration](02-configuration.md#cluster-configuration) and [long term store](07-long-term-store.md)
for more info about possible configurations.

Be aware that we set `mapping.path` to `auto` for easier quickstart but this option is not production friendly.
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

To launch seq-db with seq-ui you need to add minimal configuration file for seq-ui, [config.seq-ui.yaml](https://github.com/ozontech/seq-db/blob/main/quickstart/config.seq-ui.yaml):

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

And then launch using this minimal [docker compose example](https://github.com/ozontech/seq-db/blob/main/quickstart/docker-compose.seq-ui.yaml):

```yaml
services:

  seq-ui:
    image: ghcr.io/ozontech/seq-ui:latest
    volumes:
      - ${PWD}/config.seq-ui.yaml:/seq-ui/config.yaml
    ports:
      - "5555:5555" # Default HTTP port
      - "5556:5556" # Default gRPC port
      - "5557:5557" # Default debug port
    command: --config config.yaml

  seq-ui-fe:
    image: ghcr.io/ozontech/seq-ui-fe:latest
    ports:
      - "5173:80"
  
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
- Built-in support for various types of aggregations: sum, avg, quantiles etc - [aggregations](13-aggregations.md)
- The ability to combine multiple aggregations into a single request using [complex-search](10-public-api.md#complexsearch)
- Document-ID based retrieval can be [fetched](10-public-api.md#fetch)
