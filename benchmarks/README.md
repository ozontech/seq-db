# Benchmarks

## How to run benchmarks?

First things first you want to run following command:
```bash
make prepare-dataset
```

This command downloads logs dataset, which Elasticsearch uses in their [benchmarks](https://elasticsearch-benchmarks.elastic.co/) and distributes them equally among `10` files (you can control this via `--num-files` or `-n` flag).
You can find more detailed information about logs in dataset right [here](https://github.com/elastic/rally-tracks/blob/master/http_logs/README.md).

Be aware, that command may execute for a while because it deals with around 30 GiB of data.

We decided to split benchmarks into separate suites, because `seq-db`, Elasticsearch, and VictoriaLogs may interfere with each other causing performance degradations.

So, in order to start `seq-db` benchmark suite, you need to run following command:
```bash
make docker-run-seqdb
```

In order to start Elasticsearch benchmark suite, you need to run following command:
```bash
make docker-run-elastic
```

In order to start VictoriaLogs benchmark suite, you need to run following command:
```bash
make docker-run-vlogs
```

It will start up necessary containers and then you can observe metrics on Grafana [dashboard](http://localhost:3000/).

## How it works?

There are several the most important containers:
- `seq-db` - seq-db container that processes logs (version `v0.61.0`);
- `elastic` - Elasticsearch container that processes logs (version `v8.17.4`);
- `vlogs` - VictoriaLogs container that processes logs (version `v1.49.0`);
- `logs-loader` - Go-based logs loader which sends logs via Elasticsearch bulk API

This directory has following layout which is explained in comments:
```text
.
├── configs  # Configuration files for all containers
│  ├── du
│  ├── elasticsearch
│  ├── grafana
│  ├── seqdb
│  └── vmsingle
├── k6  # Here you can find k6 scripts for testing search performance
├── dataset  # Here you can find logs which will be ingested by ElasticSearch and seq-db
│  ├── logs
│  └── distribute.py
├── docker-compose-seqdb.yml  # File that contains `seq-db` and `logs-loader` containers
├── docker-compose-elastic.yml  # File that contains `elastic` and `logs-loader` containers
├── docker-compose-victorialogs.yml  # File that contains `vlogs` (VictoriaLogs) and `logs-loader` containers
├── docker-compose.yml  # File that contains all infra containers (different metric exporters)
└── Makefile
```

### Benchmarking search performance

Once data is loaded to the chosen database, one may want to run search benchmarks in k6 directory. Please consult the corresponding [README.md file](https://github.com/ozontech/seq-db/blob/main/benchmarks/k6/README.md).

## Results

We tested seq-db and Elasticsearch against synthetic dataset and our real production logs.
You can find detailed information about results right [here](https://ozontech.github.io/seq-db-docs/seq-db/benchmarks/).

---

These benchmarks were heavily inspired by VictoriaLogs benchmarks.
You can find them and learn more about them right [here](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/logs-benchmark).
