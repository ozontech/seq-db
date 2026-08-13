#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: DB=seqdb|vlogs $0 <docs-dir>" >&2
  exit 1
fi

DOCS_DIR="$(cd "$1" && pwd)"

LOGS_SENDER="${SCRIPT_DIR}/bin/logs-sender"
if [[ ! -x "${LOGS_SENDER}" ]]; then
  echo "logs-sender binary not found at ${LOGS_SENDER}" >&2
  echo "Build it first (as your normal user, not under sudo):" >&2
  echo "  cd ${SCRIPT_DIR} && mkdir -p bin && go build -o bin/logs-sender ./cmd/logs-sender" >&2
  exit 1
fi

: "${FILE_PATTERN:=*docs.unpacked}"
: "${INDEX_NAME:=logs-index}"
: "${BULK_SIZE:=512}"
: "${QUEUE_CAPACITY:=20}"
: "${SENDERS:=8}"
: "${REQUEST_TIMEOUT:=30s}"

export DATASET_DIR="${DOCS_DIR}"
export BULK_URL
export FILE_PATTERN
export INDEX_NAME
export BULK_SIZE
export QUEUE_CAPACITY
export SENDERS
export REQUEST_TIMEOUT

echo "Loading docs from ${DOCS_DIR} into ${DB} (${BULK_URL})"
"${LOGS_SENDER}"
