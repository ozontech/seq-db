# seqbazooka reference

`seqbazooka` is a load/benchmark driver for seq-db. It boots a seq-db **docker
container** (by default in the **host network namespace**), runs a scenario
against it, and writes a JSON report of per-operation latencies

Every scenario has the same lifecycle: **Setup** → **Execute** (bounded by
`--duration`) → **Teardown** (always runs; writes the report, then removes the
container). Teardown removes the container **and its data volume**.

## seq-db ports (host network → localhost)

| Port | What | Used for |
|------|------|----------|
| `9002` | HTTP API | `/_bulk` ingest, `/complex-search` |
| `9200` | debug server (`address.debug`) | `/ready`, `/live`, `/metrics`, `/debug/pprof/*`, `/debug/fgprof` |

pprof and Prometheus metrics are **always on** and live on the debug port. If the
bootstrap config sets `SEQDB_ADDRESS_DEBUG` to a non-default value, adjust the
port accordingly. The seq-db container is named `seqbench-seqdb[-<suffix>]`.

## Bootstrap (how seq-db is started)

- `--bootstrap.image=<tag>` — run a prebuilt image. **Takes precedence over
  `--bootstrap.reference`.** This is how you benchmark **local code**: build an
  image from the working tree and pass its tag here (`--bootstrap.reference`
  only fetches published git refs from GitHub and cannot see local edits).
- `--bootstrap.reference=<ref>` — build from a seq-db git branch/commit/tag
  (downloads the GitHub tarball, builds `build/package/Dockerfile`).
- `--bootstrap.config=<path>` — **required.** An env-style `key=value` file
  (NOT YAML; `#` comments allowed), schemes `file://`, `https://`, or bare path.
  Its keys become the container's **environment variables** (seq-db is
  configured via `SEQDB_*` env vars). seq-db also gets a hardcoded
  `api: {es_version: 8.9.0}` YAML and an embedded `/etc/mapping.yaml`.
- `--bootstrap.network=host` (default) — exposes `9002` and `9200` on localhost.
- `--bootstrap.cpu=4`, `--bootstrap.memory=8GiB` — container limits. **Pin these
  across runs** so measurements are comparable.
- `--bootstrap.suffix=<s>` — container name suffix.
- `--bootstrap.data-volume=<name>` — named volume at `/var/seqdb`. Note: cleanup
  force-removes volumes, so data does not survive a normal run.

## Global / report flags

- `--duration=<dur>` — max duration of the **execute** phase (`0` = unbounded).
  Setup/teardown are not bounded by it.
- `--report.path=<path>` (default `report.seqbazooka`).
- `--report.format=json|md|gfm` (default `json`).

## Commands

All search/aggregation commands first ingest via `--bulk.*`, then benchmark.

### `bulk` — write load
The standalone `bulk` command uses **unprefixed** flags (the `--bulk.*` prefix
below is only for scenarios where bulk is a nested ingest phase): `--limit`
(req/s, required), `--workers` (required), `--bulk-size` (docs/request,
required), `--dataset-size` (total docs, `0`=infinite until `--duration`).

### `mixed` — concurrent write + sliding-window search
`--bulk.*` plus `--search-sliding-window.limit` (req/s, required),
`--search-sliding-window.workers` (required), `--search-sliding-window.size`
(docs/req, default 1), `--search-sliding-window.window` (default `1m`).

### `search-logbench` — catalog, cold+hot
Ingests once, then benchmarks a built-in catalog of ~36 queries (filters, regex,
`in()`, ranges, histograms, aggregations), each sampled **cold then hot**.
`--bulk.*` plus `--size` (docs/req, default 1), `--iterations` (passes, default
1), `--repeats` (hot samples per query after 1 cold, default 5).

### `search-regular` — one query in a loop
`--bulk.*` plus `--query` (seqql, required), `--cold` (restart+flush before each
sample), `--workers` (default 1, hot only), `--limit` (default 1000, hot only),
`--size` (default 1).

### `search-aggregation` — one aggregation in a loop
`--bulk.*` plus `--func` (`count|sum|avg|min|max|quantile|unique`, required),
`--field` (repeatable; first aggregated, rest group-by), `--query` (filter,
default `*`), plus `--cold`/`--workers`/`--limit`/`--size` as above.

**cold vs hot**: cold = measured right after page-cache flush + container
restart (empty OS + in-process caches); hot = warm caches. In `--cold`
single-query modes `--workers`/`--limit` are ignored (sequential samples).

## Dataset & document schema (needed to write `--query`)

seqbazooka ingests **synthetic, deterministic** logs (seed `"SEQMAGIC"×4`), not
your data. To write a meaningful `--query` you must target fields and values that
actually exist. The generator models 3 apps × 7 regions = **21 services**, 10
pods each (**210 pods**), all in namespace `prod`:

- apps (→ `k8s_container`): `payment-backend`, `payment-frontend`, `api-gateway`
- regions: `eu`, `ru`, `us`, `uz`, `kaz`, `aus`, `ch`
- `service` = `<app>-<region>` (e.g. `payment-backend-eu`); `k8s_pod` =
  `<app>-<region>-<0..9>` (e.g. `api-gateway-us-4`).

Example documents (fields are `omitempty`, so which appear depends on the app):

```json
// api-gateway
{"timestamp":"2026-09-09T12:34:56.789Z","k8s_node":"node-pool-b-q9v5t","k8s_namespace":"prod","k8s_pod":"api-gateway-us-4","k8s_container":"api-gateway","level":6,"service":"api-gateway-us","span_id":"j3k9f2m8s1x7q4b0","trace_id":"9f2a1c...(32 hex)","client_ip":"203.0.113.45","resource":"/api/v1/payment","status":200,"size":128374,"method":"POST","message":"resource access logged"}

// payment-backend (ERROR); payment-frontend has the same shape, different messages
{"timestamp":"2026-09-09T12:34:56.789Z","k8s_node":"node-pool-a-r8n1j","k8s_namespace":"prod","k8s_pod":"payment-backend-eu-2","k8s_container":"payment-backend","level":3,"service":"payment-backend-eu","span_id":"k3f9...(16)","trace_id":"a1b2...(32 hex)","client_ip":"10.2.3.4","transaction_id":"tx-1a2b3c4d-9f0a","user_id":"user-k3z9a","order_id":"ORD-9a8b7c6d","message":"payment authorization failed: insufficient funds: missing 512 coins"}
```

### Field cardinality (drives query selectivity — pick deliberately)

`level` is stored as an **integer** (syslog): `3`=ERROR, `4`=WARNING, `6`=INFO,
`7`=DEBUG (weighted 67% INFO / 20% WARN / 9% ERROR / 4% DEBUG). `message` is the
only **`text`** (tokenized full-text) field; everything else is **`keyword`**
(exact-term match). `status`/`size` are keyword but hold integers.

| field | type | present in | cardinality | example |
|---|---|---|---|---|
| `k8s_namespace` | keyword | all | **1** (`prod`) — matches everything, useless as filter | `prod` |
| `k8s_container` | keyword | all | **3** | `api-gateway` |
| `level` | keyword(int) | all | **4** (3/4/6/7) | `6` |
| `method` | keyword | api-gateway | **7** | `GET` |
| `k8s_node` | keyword | all | **6** (fixed pool) | `node-pool-a-x4k2m` |
| `status` | keyword(int) | api-gateway | **3** (200/400/404, 80/10/10%) | `200` |
| `service` | keyword | all | **21** — the natural "prod-like" selective filter | `payment-backend-eu` |
| `resource` | keyword | api-gateway | **63** | `/api/v1/payment` |
| `k8s_pod` | keyword | all | **210** | `payment-backend-eu-3` |
| `user_id` | keyword | payment-* | high (~`user-`+5 alnum) | `user-k3z9a` |
| `size` | keyword(int) | api-gateway, status 200 only | high (0..5 MiB) | `128374` |
| `client_ip` | keyword | all | very high (random IPv4) | `203.0.113.45` |
| `order_id` | keyword | payment-* | ~per-doc (`ORD-`+8) | `ORD-9a8b7c6d` |
| `span_id` / `trace_id` | keyword | all | ~per-doc (16 / 32 chars) | `a1b2…` |
| `transaction_id` | keyword | payment-* | ~per-doc, **plus a needle** (below) | `tx-1a2b3c4d-9f0a` |
| `message` | text | all | templated per level/app; **constant** `resource access logged` for api-gateway | — |

**Needle**: every 10000th doc per pod sets `transaction_id` to
`tx-needle-<4 hex>` — a deliberately rare value for benchmarking highly
selective search (`transaction_id:tx-needle-*`).

### Writing queries by target selectivity

- **Broad / most docs** (stress fan-out & scan): `level:6`, `k8s_container:api-gateway`.
- **Selective, prod-like** (the common real query): `service:payment-backend-eu`,
  optionally `and level:3`.
- **Field-specific**: `method:POST and status:404` (api-gateway),
  `resource:/api/v1/payment`, `size:[1000000, 5242880]` (range).
- **Full-text on `message`**: `message:'payment authorization failed'`,
  `message:timeout`.
- **`in()` sets**: `service:in(payment-backend-eu, payment-backend-us)`.
- **Rare / needle** (highly selective): `transaction_id:tx-needle-*`.

For aggregation scenarios, group by a low/medium-cardinality keyword
(`service`, `k8s_pod`, `method`, `status`), e.g. `--func=count --field=service`
→ `* | group by (service) | count`. Grouping by a per-doc field
(`trace_id`, `order_id`) explodes cardinality — use only to stress that path on
purpose.

## The report (the deliverable to compare across runs)

JSON = an **array** of measurement objects:

```json
[
  {
    "query": "<seqql dump, or \"bulk\">",
    "type": "hot",          // GOTCHA: this is temperature (hot|cold), NOT query kind
    "metrics": [
      {"name": "mean (ms)",   "value": 1.23},
      {"name": "stddev (ms)", "value": 0.45},
      {"name": "p(50) (ms)",  "value": 1.10},
      {"name": "p(95) (ms)",  "value": 2.00},
      {"name": "p(99) (ms)",  "value": 3.50},
      {"name": "iterations",  "value": 500},   // sample count (reservoir-capped at 16384)
      {"name": "total",       "value": 100000} // seq-db with_total match count; 0 for bulk
    ]
  }
]
```

The 7 metrics are always in this order. **`value` can be `null`** (NaN / too few
samples). All latencies are **ms**. mean/stddev use IQR outlier removal
(≥8 samples); percentiles use raw values. `iterations` keeps counting past the
16384 sample cap; `total` is 0 for `bulk`.

Extract for comparison, e.g.:
```sh
jq -r '.[] | "\(.query)\t\(.type)\t\([.metrics[]|select(.name=="p(99) (ms)").value]|.[0])"' report.json
```

## Collecting seq-db profiles/metrics (via the observability stack)

seqbazooka only measures **client-side** latency and does not collect anything
from seq-db. The perf-loop runs a small observability stack that watches seq-db
**continuously for the whole run** (far more robust than a single mid-run
snapshot) — Prometheus for metrics, Pyroscope (fed by Grafana Alloy) for
profiles. Bring it up once and leave it running across iterations:

```sh
.claude/skills/perf-loop/scripts/stack.sh up      # also: status | down | nuke
```

It exposes: **Prometheus `localhost:9090`** (scrapes seq-db `/metrics` every 5s),
**Pyroscope `localhost:4040`** (continuous CPU/heap/allocs/goroutine profiles),
**Alloy `localhost:12345`**. seq-db is only up while a run executes, so these
targets are simply down between runs.

**Record each run's time window.** seqbazooka has no notion of the stack, so note
the epoch seconds just before and after the run (`date +%s`) — you slice both
Prometheus and Pyroscope by `[from,to]`. Setup/ingest ramp is included in the
window; narrow the range if you want steady-state only.

Two scripts wrap the fiddly `docker`/`profilecli`/`curl` details (and are the
allowlisted entry points — use them rather than raw commands):

**Metrics** — `scripts/promql.sh <from> <to> <promql> [step]` range-queries the
local Prometheus and prints raw JSON (pipe to `jq`). Metrics are namespaced
`seq_db_*` (e.g. `seq_db_main_seals_total`, `seq_db_ingestor_*`,
`seq_db_store_compaction_*`, `seq_db_common_bytes_pool_*`) plus Go runtime `go_*`:
```sh
.claude/skills/perf-loop/scripts/promql.sh "$FROM" "$TO" 'rate(seq_db_main_seals_total[1m])' | jq .
```

**Profiles** — `scripts/collect-profiles.sh <out_dir> <from> <to> [service]`
exports merged pprof for the window (CPU, allocs, heap, goroutine) from
Pyroscope, ready for `go tool pprof`:
```sh
.claude/skills/perf-loop/scripts/collect-profiles.sh .perf-loop/001-fix "$FROM" "$TO"
```

## Example invocations

Write scenario against a locally built image, 10-minute execute:
```sh
seqbazooka bulk \
  --bootstrap.config=file://./seqdb.env \
  --bootstrap.image=seq-db:perf-local \
  --bootstrap.network=host --bootstrap.cpu=4 --bootstrap.memory=8GiB \
  --duration=10m \
  --limit=200 --workers=16 --bulk-size=1000 --dataset-size=0 \
  --report.path=./report.json --report.format=json
```

Single-query search, ingest 1M docs then hot-loop:
```sh
seqbazooka search-regular \
  --bootstrap.config=file://./seqdb.env --bootstrap.image=seq-db:perf-local \
  --bulk.limit=200 --bulk.workers=8 --bulk.bulk-size=500 --bulk.dataset-size=1000000 \
  --query='service:payment AND k8s_namespace:prod' \
  --workers=4 --limit=500 --size=10 --duration=5m \
  --report.path=./report.json
```

## Gotchas

1. `--bootstrap.config` is an env `key=value` file, not YAML.
2. Benchmarking local edits requires a **locally built image** + `--bootstrap.image`
   (`VERSION=perf-local make build-image` → `ghcr.io/ozontech/seq-db:perf-local`).
   testcontainers **pulls from the registry if the image is absent locally**, which
   fails with `manifest unknown` for this never-pushed tag — verify
   `docker image inspect <tag>` succeeds before a run.
3. Report `"type"` = temperature (hot/cold), not query kind.
4. Report `value` can be `null`; `total` is 0 for `bulk`.
5. Pin `--bootstrap.cpu/--bootstrap.memory` and scenario params across runs, or
   comparisons are meaningless.
6. Teardown destroys the container + volume. Profiles/metrics are captured
   continuously by the observability stack, so nothing to grab mid-run — but note
   the run's `[from,to]` epochs to slice them afterwards.
