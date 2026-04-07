# K6 Benchmarks

### Methodology

- Requests are issued in parallel by multiple workers (VUs) to simulate concurrent usage of the database.
- For ElasticSearch request cache is disabled as it caches direct search results, not just data blocks.
- VictoriaLogs returns logs unsorted by default. However, most of the time a typical user wants to
  page through logs sorted by time, i.e. find a top N recent logs and page deeper (next top N recent logs).
  Therefore, paging scenarios has additional `sort by (_time) asc` pipes. This is exactly how official
  VictoriaLogs documentation [recommends](https://docs.victoriametrics.com/victorialogs/logsql/#logsql-tutorial) to query logs. At the same time, analytics (histogram, aggregation)
  queries do not have the sort pipe. Moreover, VictoriaLogs already stores logs sorted by time (there is only a single
  log stream), so this should not penalize search performance.

## Preparation

### Test Dataset Preparation
Before running the benchmarks, you must start the seq-db instance and load the test data into it. Instructions for setup and data loading are provided in the [document](/benchmarks/README.md).

### Installing k6
```bash
# For MacOS (via Homebrew)
brew install k6

# Alternative installation methods:
# https://grafana.com/docs/k6/latest/set-up/install-k6/
```

## Running Benchmarks

### Basic Execution
```bash
BASE_URL=http://localhost:9002 k6 run <script_name>.js
```

### Running Against Different Environments
Replace the `BASE_URL` value depending on your target environment:

```bash
BASE_URL=https://api.example.com k6 run script.js
```

VictoriaLogs example:

```bash
BASE_URL=http://localhost:9428 k6 run vlogs-fetch-5k.js
```

---

*For more detailed configuration, please refer to the [official k6 documentation](https://k6.io/docs/)*