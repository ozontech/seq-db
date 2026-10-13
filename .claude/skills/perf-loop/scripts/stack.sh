#!/usr/bin/env bash
# Manage the perf-loop observability stack (Prometheus + Pyroscope + Alloy).
# Bring it up ONCE per session and keep it running across iterations so profiles
# and metrics accumulate and runs stay comparable.
#
# usage: stack.sh [up|down|nuke|status]
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="$HERE/../observability/docker-compose.perf.yml"

case "${1:-up}" in
  up)
    docker compose -f "$COMPOSE" up -d || exit 1
    printf "waiting for prometheus"
    until curl -fsS localhost:9090/-/ready >/dev/null 2>&1; do printf .; sleep 2; done
    echo " ok"
    # Pyroscope delays readiness ~15-30s after start (503 until then); that's benign.
    printf "waiting for pyroscope"
    for _ in $(seq 1 30); do
      if curl -fsS localhost:4040/ready >/dev/null 2>&1; then break; fi
      printf .; sleep 2
    done
    echo " ok"
    ;;
  down)   docker compose -f "$COMPOSE" down ;;
  nuke)   docker compose -f "$COMPOSE" down -v ;;   # also drops metric/profile history
  status) docker compose -f "$COMPOSE" ps ;;
  *) echo "usage: stack.sh [up|down|nuke|status]"; exit 1 ;;
esac
