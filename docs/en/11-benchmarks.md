---
id: benchmarks
---

# Benchmarks

## Methodology

We prepared a set of benchmarks designed to be as reproducible and deterministic as possible.  
The dataset used during benchmarks is deterministic and consists of 40GiB (219 million) structured JSON logs.  

Example log:
```json
{
  "@timestamp": 897484581,
  "clientip": "162.146.3.0",
  "request": "GET /images/logo_cfo.gif HTTP/1.1",
  "status": 200,
  "size": 1504
}
```

All benchmarks were run against this dataset. The only thing that was changing - computational resources and cluster size.

## Local Deploy

Tests were run on an AWS host `c6a.4xlarge`, with the following configuration:

| CPU                                    | RAM   | Disk |
|----------------------------------------|-------|------|
| AMD EPYC 7R13 Processor 3.6GHz, 16vCPU | 32GiB | GP3  |

For more details on component setup and how to run the suite, see the [link](https://github.com/ozontech/seq-db/tree/master/benchmarks).

Local cluster configuration:

| Container                | Replicas | CPU Limit | RAM Limit |
|--------------------------|----------|-----------|-----------|
| seq‑db (`--mode single`) | 1        | 4         | 8GiB      |
| elasticsearch            | 1        | 4         | 8GiB      |
| file.d                   | 1        | –         | 12GiB     |

### Results (write‑path)

In the synthetic tests we obtained the following results:

| Container     | Avg. Logs/sec | Avg. Throughput | Avg. CPU Usage | Avg. RAM Usage |
|---------------|---------------|-----------------|----------------|----------------|
| seq‑db        | 370,000       | 48MiB/s         | 3.3vCPU        | 1.8GiB         |
| elasticsearch | 110,000       | 14MiB/s         | 1.9vCPU        | 2.4GiB         |

Thus, with comparable resource usage, seq‑db demonstrated on average 3.4× higher throughput than Elasticsearch.

### Results (read‑path)

Both stores were pre-loaded with the same dataset. Read-path tests were run without any write load.

Elasticsearch settings:
- Request cache disabled (`request_cache=false`)
- Total hits counting disabled (`track_total_hits=false`)

Tests were executed using [Grafana k6](https://k6.io/), with query parameters available in the [benchmarks/k6](https://github.com/ozontech/seq-db/tree/main/benchmarks/k6) folder.

#### Scenario: fetch all logs using offsets

ES enforces default limit `page_size * offset ≤ 10,000`.

Parameters: 20 looping virtual users for 10s, fetching a random page [1–50].

| DB            | Avg    | P50    | P95    |
|---------------|--------|--------|--------|
| seq‑db        | 5.56ms | 5.05ms | 9.56ms |
| elasticsearch | 6.06ms | 5.11ms | 11.8ms |

#### Scenario `status: in(500,400,403)`

Parameters: 20  VUs for 10s.

| DB            | Avg      | P50      | P95      |
|---------------|----------|----------|----------|
| seq‑db        | 364.68ms | 356.96ms | 472.26ms |
| elasticsearch | 21.68ms  | 18.91ms  | 29.84ms  |

#### Scenario `request: GET /english/images/top_stories.gif HTTP/1.0`

Parameters: 20 looping VUs for 10s.

| DB            | Avg      | P50      | P95      |
|---------------|----------|----------|----------|
| seq‑db        | 269.98ms | 213.43ms | 704.19ms |
| elasticsearch | 46.65ms  | 43.27ms  | 80.53ms  |

#### Scenario: aggregation counting logs by status

SQL analogue: `SELECT status, COUNT(*) GROUP BY status`.  
Parameters: 10 parallel queries, 2 VUs.

| DB            | Avg    | P50    | P95    |
|---------------|--------|--------|--------|
| seq‑db        | 16.81s | 16.88s | 16.10s |
| elasticsearch | 6.46s  | 6.44s  | 6.57s  |

#### Scenario: minimum log size for each status

SQL analogue: `SELECT status, MIN(size) GROUP BY status`.  
Parameters: 5 iterations with 1 thread.

| DB            | Avg    | P50    | P95    |
|---------------|--------|--------|--------|
| seq‑db        | 33.34s | 33.41s | 33.93s |
| elasticsearch | 16.88s | 16.82s | 17.5s  |

#### Scenario: range queries — fetch 5,000 documents

Parameters: 20 threads, 10s, random page [1–50], 100 documents per page.

| DB            | Avg      | P50      | P95      |
|---------------|----------|----------|----------|
| seq‑db        | 406.09ms | 385.13ms | 509.05ms |
| elasticsearch | 22.75ms  | 18.06ms  | 64.61ms  |

## K8S Deploy

Cluster computation resources description:

| Container                   | CPU                       | RAM          | Disk          |
|-----------------------------|---------------------------|--------------|---------------|
| seq‑db (`--mode store`)     | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | RAID10, 4×SSD |
| seq‑db (`--mode ingestor`)  | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | –             |
| elasticsearch (master/data) | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | RAID10, 4×SSD |
| file.d                      | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | –             |

We selected a baseline set of fields to index. Elasticsearch was set up with index `k8s-logs-index` to index only those fields.

### Configuration `1x1`

Index settings (same applied to seq‑db including durability guarantees):

```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "number_of_shards": "6",
      "refresh_interval": "1s",
      "number_of_replicas": "0",
      "codec": "best_compression",
      "merge.scheduler.max_thread_count": "2",
      "translog": { "durability": "request" }
    }
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "request": { "type": "text" },
      "size": { "type": "keyword" },
      "status": { "type": "keyword" },
      "clientip": { "type": "keyword" }
    }
  }
}'
```

#### Results

| Container               | CPU Limit | RAM Limit | Avg. CPU | Avg. RAM |
|-------------------------|-----------|-----------|----------|----------|
| seq‑db (`--mode store`) | 10        | 16GiB     | 8.81     | 3.2GB    |
| seq‑db (`--mode proxy`) | 6         | 8GiB      | 4.92     | 4.9 GiB  |
| elasticsearch (data)    | 16        | 32GiB     | 15.18    | 13GB     |

| Container     | Avg. Throughput | Logs/sec  |
|---------------|-----------------|-----------|
| seq‑db        | 181MiB/s        | 1,403,514 |
| elasticsearch | 61MiB/s         | 442,924   |

Here, seq‑db achieved ~2.9x higher throughput with fewer resources usage.

### Configuration `6x6`

Six seq‑db instances with `--mode proxy` and six with `--mode store`. 
Elasticsearch indexing settings stayed the same except `number_of_replicas=1`:

```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "number_of_shards": "6",
      "refresh_interval": "1s",
      "number_of_replicas": "1",
      "codec": "best_compression",
      "merge.scheduler.max_thread_count": "2",
      "translog": { "durability": "request" }
    }
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "request": { "type": "text" },
      "size": { "type": "keyword" },
      "status": { "type": "keyword" },
      "clientip": { "type": "keyword" }
    }
  }
}'
```

#### Results

| Container                | CPU Limit | RAM Limit | Replicas | Avg. CPU (per instance) | Avg. RAM (per instance) |
|--------------------------|-----------|-----------|----------|-------------------------|-------------------------|
| seq‑db (`--mode proxy`)  | 3         | 8GiB      | 6        | 1.87                    | 2.2GiB                  |
| seq‑db (`--mode store`)  | 10        | 16GiB     | 6        | 7.40                    | 2.5GiB                  |
| elasticsearch (data)     | 13        | 32GiB     | 6        | 7.34                    | 8.8GiB                  |

| Container     | Avg. Throughput | Avg. Logs/sec  |
|---------------|-----------------|----------------|
| seq‑db        | 436MiB/s        | 3,383,724      |
| elasticsearch | 62MiB/s         | 482,596        |
