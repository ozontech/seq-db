#!/usr/bin/env bash
# Range-query seq-db metrics from the local Prometheus over a time window.
# Prints the raw Prometheus JSON (pipe to jq). Constrained to localhost:9090
# so it is safe to allowlist (no arbitrary curl).
#
# usage: promql.sh <from_epoch> <to_epoch> <promql> [step_seconds]
#   e.g. promql.sh 1788976537 1788976666 'rate(seq_db_main_seals_total[1m])' 10
set -uo pipefail

FROM="${1:?usage: promql.sh <from_epoch> <to_epoch> <promql> [step_seconds]}"
TO="${2:?missing to_epoch}"
QUERY="${3:?missing promql}"
STEP="${4:-10}"
PROM="${PROM:-http://localhost:9090}"

curl -s -G "$PROM/api/v1/query_range" \
  --data-urlencode "query=$QUERY" \
  --data-urlencode "start=$FROM" \
  --data-urlencode "end=$TO" \
  --data-urlencode "step=$STEP"
