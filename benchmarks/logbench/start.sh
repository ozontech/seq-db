#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

mkdir -p "${DATA_DIR}"

if docker container inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
  echo "Starting existing container ${CONTAINER_NAME}"
  docker start "${CONTAINER_NAME}" >/dev/null
  exit 0
fi

echo "Creating container ${CONTAINER_NAME} from ${IMAGE}"

case "${DB}" in
  seqdb)
    SEQDB_MAPPING_FILE_PATH=${SEQDB_MAPPING_FILE:=mapping-prod.yaml}
    echo "Using mapping file ${SEQDB_MAPPING_FILE_PATH}"

    docker run -d \
      --name "${CONTAINER_NAME}" \
      --cpus="${CPUS}" \
      --memory="${MEMORY}" \
      -p "${HTTP_PORT}:9002" \
      -p "${DEBUG_PORT}:9200" \
      -v "${DATA_DIR}:/seq-db-data" \
      -v "${CONFIG_DIR}:/configs" \
      -v "./${SEQDB_MAPPING_FILE_PATH}:/configs/mapping.yaml" \
      "${IMAGE}" \
      --mode=single \
      --config=/configs/config.yaml \
      >/dev/null
    ;;
  vlogs)
    docker run -d \
      --name "${CONTAINER_NAME}" \
      --cpus="${CPUS}" \
      --memory="${MEMORY}" \
      -p "${HTTP_PORT}:9428" \
      -v "${DATA_DIR}:/vlogs" \
      "${IMAGE}" \
      --storageDataPath=/vlogs \
      --insert.maxLineSizeBytes=600144 \
      --retentionPeriod=1000d \
      >/dev/null
    ;;
esac
