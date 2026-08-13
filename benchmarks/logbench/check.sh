#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

case "${DB}" in
  seqdb)
    curl -sf "http://127.0.0.1:${DEBUG_PORT}/ready" >/dev/null
    ;;
  vlogs)
    curl -sf "http://127.0.0.1:${HTTP_PORT}/health" >/dev/null
    ;;
esac
