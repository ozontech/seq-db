#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

if ! docker container inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
  echo "Container ${CONTAINER_NAME} does not exist"
  exit 0
fi

echo "Stopping container ${CONTAINER_NAME}"
docker stop --time=120 "${CONTAINER_NAME}" >/dev/null || true
