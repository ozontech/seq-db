---
id: benchmarks
---

# Benchmarks

## Synthetic Data
### Methodology

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

| Container     | Avg Logs/sec | Avg Throughput | Avg CPU Usage | Avg RAM Usage |
|---------------|--------------|----------------|---------------|---------------|
| seq‑db        | 370,000      | 48MiB/s        | 3.3vCPU       | 1.8GiB        |
| elasticsearch | 110,000      | 14MiB/s        | 1.9vCPU       | 2.4GiB        |

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

## Real (production) Data
### Methodology

In addition to synthetic tests, we also seq‑db and Elasticsearch on real logs from our production services. We prepared several benchmark scenarios showing performance for a single instance and for a medium‑sized cluster:

- Throughput test of a single instance of seq‑db and Elasticsearch (`1x1` configuration)
- Throughput test of 6 instances of seq‑db and Elasticsearch with RF=2 (`6x6` configuration)

Real production log datasets (~280GiB total) were pre-processed to minimize CPU use during ingestion via file.d to seq‑db or Elasticsearch. 
This ensured determinism and independence from delivery systems (e.g., Apache Kafka).

The high level write pipeline:
```
┌──────────────┐        ┌───────────┐  
│ ┌──────┐     │        │           │  
│ │ file ├──┐  │    ┌──►│  elastic  │  
│ └──────┘  │  │    │   │           │  
│ ┌─────────▼┐ │    │   └───────────┘  
│ │          ├─┼────┘   ┌───────────┐  
│ │  file.d  │ │        │           │  
│ │          ├─┼───────►│  seq-db   │  
│ └──────────┘ │        │           │  
└──────────────┘        └───────────┘  
```

Cluster configuration:

| Container                       | CPU                       | RAM          | Disk                      |
|---------------------------------|---------------------------|--------------|---------------------------|
| seq‑db (`--mode store`)         | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | RAID10, 4×SSD             |
| seq‑db (`--mode ingestor`)      | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | –                         |
| elasticsearch (master/data)     | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | RAID10, 4×SSD             |
| file.d                          | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | –                         |

We selected a baseline set of fields to index. Elasticsearch was set up with index `k8s-logs-index` to index only those fields.

#### Configuration `1x1`

Index settings (same applied to seq‑db):

```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": { "codec": "best_compression" },
    "number_of_replicas": 0,
    "number_of_shards": 6,
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "k8s_cluster": { "type": "keyword" },
      "k8s_container": { "type": "keyword" },
      "k8s_group": { "type": "keyword" },
      "k8s_label_jobid": { "type": "keyword" },
      "k8s_namespace": { "type": "keyword" },
      "k8s_node": { "type": "keyword" },
      "k8s_pod": { "type": "keyword" },
      "k8s_pod_label_cron": { "type": "keyword" },
      "client_ip": { "type": "keyword" },
      "http_code": { "type": "integer" },
      "http_method": { "type": "keyword" },
      "message": { "type": "text" }
    }
  }
}'
```

##### Results

| Container               | CPU Limit | RAM Limit | Avg CPU | Avg RAM |
|-------------------------|-----------|-----------|---------|---------|
| seq‑db (`--mode store`) | 8         | 16GiB     | 6.5     | 7GiB    |
| seq‑db (`--mode proxy`) | 8         | 8GiB      | 7       | 3GiB    |
| elasticsearch (master)  | 2         | 4GiB      | 0       | 0GiB    |
| elasticsearch (data)    | 16        | 32GiB     | 15.8    | 30GiB   |

| Container      | Avg Throughput | Logs/sec |
|----------------|----------------|----------|
| seq‑db         | 520MiB/s       | 162,000  |
| elasticsearch  | 195MiB/s       | 62,000   |

Here, seq‑db achieved ~2.6x higher throughput under similar resource constraints.

#### Configuration `6x6`

Six seq‑db nodes in `--mode proxy` and six in `--mode store`. 
Elasticsearch indexing settings stayed the same except `number_of_replicas=1`:
```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": { "codec": "best_compression" },
    "number_of_replicas": 1,
    "number_of_shards": 6,
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "k8s_cluster": { "type": "keyword" },
      "k8s_container": { "type": "keyword" },
      "k8s_group": { "type": "keyword" },
      "k8s_label_jobid": { "type": "keyword" },
      "k8s_namespace": { "type": "keyword" },
      "k8s_node": { "type": "keyword" },
      "k8s_pod": { "type": "keyword" },
      "k8s_pod_label_cron": { "type": "keyword" },
      "client_ip": { "type": "keyword" },
      "http_code": { "type": "integer" },
      "http_method": { "type": "keyword" },
      "message": { "type": "text" }
    }
  }
}'
```

We have also tweaked the `index.merge.scheduler.max_thread_count` to increase the bulk throughput.

##### Results

| Container                | CPU Limit | RAM Limit | Replicas | Avg CPU (per instance) | Avg RAM (per instance) |
|--------------------------|-----------|-----------|----------|------------------------|------------------------|
| seq‑db (`--mode proxy`)  | 5         | 8GiB      | 6        | 3.6                    | 1.5GiB                 |
| seq‑db (`--mode store`)  | 8         | 16GiB     | 6        | 6.1                    | 6.3GiB                 |
| elasticsearch (data)     | 13        | 32GiB     | 6        | 4.5                    | 13GiB                  |

| Container     | Avg Throughput | Logs/sec       |
|---------------|----------------|----------------|
| seq‑db        | 1.3GiB/s       | 585,139 docs/s |
| elasticsearch | 113.58MiB/s    | 37,658 docs/s  |
