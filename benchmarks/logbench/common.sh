#!/usr/bin/env bash
# Shared logbench environment. Source from other scripts after setting DB if needed.
# shellcheck disable=SC2034

: "${DB:=seqdb}"

case "${DB}" in
  seqdb|vlogs) ;;
  *)
    echo "unsupported DB=${DB} (expected seqdb or vlogs)" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "${DB}" in
  seqdb)
    : "${SEQDB_IMAGE:=ghcr.io/ozontech/seq-db:v0.76.0}"
    : "${CONTAINER_NAME:=seq-db-logbench}"
    : "${CPUS:=4}"
    : "${MEMORY:=8g}"
    : "${HTTP_PORT:=9002}"
    : "${DEBUG_PORT:=9200}"
    : "${BULK_URL:=http://127.0.0.1:${HTTP_PORT}/_bulk}"
    : "${SEARCH_BASE_URL:=http://127.0.0.1:${HTTP_PORT}}"
    : "${DEFAULT_CONTENT_TYPE:=application/json}"
    DATA_DIR="${SCRIPT_DIR}/data/seqdb"
    CONFIG_DIR="${SCRIPT_DIR}/config"
    IMAGE="${SEQDB_IMAGE}"
    ;;
  vlogs)
    : "${VLOGS_IMAGE:=victoriametrics/victoria-logs:v1.49.0}"
    : "${CONTAINER_NAME:=vlogs-logbench}"
    : "${CPUS:=4}"
    : "${MEMORY:=8g}"
    : "${HTTP_PORT:=9428}"
    : "${VLOGS_STREAM_FIELDS:=request_host,request_uri}"
    : "${BULK_URL:=http://127.0.0.1:${HTTP_PORT}/insert/elasticsearch/_bulk?_stream_fields=${VLOGS_STREAM_FIELDS}&_msg_field=message&_time_field=timestamp}"
    : "${SEARCH_BASE_URL:=http://127.0.0.1:${HTTP_PORT}}"
    : "${DEFAULT_CONTENT_TYPE:=application/x-www-form-urlencoded}"
    DATA_DIR="${SCRIPT_DIR}/data/vlogs"
    IMAGE="${VLOGS_IMAGE}"
    ;;
esac
