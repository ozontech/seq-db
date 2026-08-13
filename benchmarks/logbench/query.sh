#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

: "${CONTENT_TYPE:=${DEFAULT_CONTENT_TYPE}}"
: "${SEARCH_URL:=}"

BODY="$(cat)"

if [[ -z "${SEARCH_URL}" ]]; then
  case "${DB}" in
    seqdb)
      SEARCH_URL="${SEARCH_BASE_URL}/complex-search"
      ;;
    vlogs)
      echo "SEARCH_URL must be set for DB=vlogs (per-query path)" >&2
      exit 1
      ;;
  esac
fi

HTTP_CODE="$(
  curl -sS -o /dev/null -w '%{http_code} %{time_total}' \
    --request POST \
    --url "${SEARCH_URL}" \
    --header "Content-Type: ${CONTENT_TYPE}" \
    --data-binary "${BODY}"
)"

CODE="${HTTP_CODE%% *}"
TIME="${HTTP_CODE#* }"

if [[ "${CODE}" != "200" ]]; then
  echo "query failed with HTTP ${CODE}" >&2
  exit 1
fi

printf '%s\n' "${TIME}"
