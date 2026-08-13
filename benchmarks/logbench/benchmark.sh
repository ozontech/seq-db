#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

usage() {
  cat >&2 <<'EOF'
Usage: ./benchmark.sh [seqdb|vlogs] <docs-dir> <queries.json> [result.csv]

DB can also be set via the DB environment variable (default: seqdb).

Examples:
  sudo ./benchmark.sh ../dataset ./queries.json ./result-seqdb.csv
  sudo ./benchmark.sh vlogs ../dataset ./queries-vlogs.json ./result-vlogs.csv
  sudo DB=vlogs ./benchmark.sh ../dataset ./queries-vlogs.json
EOF
  exit 1
}

if [[ $# -ge 1 && ( "$1" == "seqdb" || "$1" == "vlogs" ) ]]; then
  export DB="$1"
  shift
fi

: "${DB:=seqdb}"
export DB

# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

if [[ $# -lt 2 ]]; then
  usage
fi

DOCS_DIR="$(cd "$1" && pwd)"
QUERIES_FILE="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
RESULT_CSV="${3:-${SCRIPT_DIR}/result-${DB}.csv}"

: "${CHECK_TIMEOUT:=300}"
: "${STOP_TIMEOUT:=60}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

if [[ ! -d "${DOCS_DIR}" ]]; then
  echo "docs directory not found: ${DOCS_DIR}" >&2
  exit 1
fi

if [[ ! -f "${QUERIES_FILE}" ]]; then
  echo "queries file not found: ${QUERIES_FILE}" >&2
  exit 1
fi

wait_ready() {
  local i
  for i in $(seq 1 "${CHECK_TIMEOUT}"); do
    if DB="${DB}" ./check.sh >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "${DB} did not become ready within ${CHECK_TIMEOUT}s" >&2
  return 1
}

wait_stopped() {
  local i
  for i in $(seq 1 "${STOP_TIMEOUT}"); do
    if ! DB="${DB}" ./check.sh >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "${DB} did not stop within ${STOP_TIMEOUT}s; proceeding anyway" >&2
  return 0
}

# For seq-db, hit GET /status (SeqProxyApi.Status) so store connectivity is
# established before the timed cold query. Not used for vlogs.
wait_status() {
  if [[ "${DB}" != "seqdb" ]]; then
    return 0
  fi

  local i body
  for i in $(seq 1 "${CHECK_TIMEOUT}"); do
    if body="$(curl -sf "http://127.0.0.1:${HTTP_PORT}/status")"; then
      if echo "${body}" | jq -e '
        (.numberOfStores // .number_of_stores // 0) > 0
        and all(.stores[]?; (.error == null) or (.error == ""))
      ' >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "seq-db /status did not become healthy within ${CHECK_TIMEOUT}s" >&2
  return 1
}

cold_cycle() {
  DB="${DB}" ./stop.sh
  wait_stopped
  ./drop-caches.sh
  DB="${DB}" ./start.sh
  wait_ready
  wait_status
}

query_payload() {
  local idx="$1"
  case "${DB}" in
    seqdb)
      jq -c ".[$idx] | del(.name)" "${QUERIES_FILE}"
      ;;
    vlogs)
      jq -r ".[$idx].body" "${QUERIES_FILE}"
      ;;
  esac
}

query_url() {
  local idx="$1"
  case "${DB}" in
    seqdb)
      printf '%s\n' "${SEARCH_BASE_URL}/complex-search"
      ;;
    vlogs)
      local path
      path="$(jq -r ".[$idx].path" "${QUERIES_FILE}")"
      printf '%s%s\n' "${SEARCH_BASE_URL}" "${path}"
      ;;
  esac
}

run_query() {
  local idx="$1"
  local body url
  body="$(query_payload "${idx}")"
  url="$(query_url "${idx}")"
  printf '%s' "${body}" | \
    SEARCH_URL="${url}" CONTENT_TYPE="${DEFAULT_CONTENT_TYPE}" DB="${DB}" ./query.sh
}

min_time() {
  awk -v a="$1" -v b="$2" 'BEGIN {
    if (a + 0 < b + 0) print a; else print b
  }'
}

echo "=== logbench (${DB}): start ==="
DB="${DB}" ./start.sh
wait_ready

echo "=== logbench (${DB}): load data ==="
DB="${DB}" ./load.sh "${DOCS_DIR}"
sync

echo "=== logbench (${DB}): waiting 60s after load ==="
sleep 60

echo "query,cold_s,hot_s" > "${RESULT_CSV}"

QUERY_COUNT="$(jq 'length' "${QUERIES_FILE}")"
echo "=== logbench (${DB}): running ${QUERY_COUNT} queries ==="

for i in $(seq 0 $((QUERY_COUNT - 1))); do
  NAME="$(jq -r ".[$i].name" "${QUERIES_FILE}")"

  echo "--- query: ${NAME} (cold) ---"
  cold_cycle
  COLD="$(run_query "${i}")"

  echo "--- query: ${NAME} (hot x2) ---"
  HOT1="$(run_query "${i}")"
  HOT2="$(run_query "${i}")"
  HOT="$(min_time "${HOT1}" "${HOT2}")"

  echo "${NAME},${COLD},${HOT}" | tee -a "${RESULT_CSV}"
  echo "cold=${COLD}s hot=${HOT}s (min of ${HOT1}, ${HOT2})"
done

echo "=== logbench (${DB}): stop ==="
DB="${DB}" ./stop.sh

echo "Results written to ${RESULT_CSV}"
