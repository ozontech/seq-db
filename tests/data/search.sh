#!/bin/bash

set -euxo pipefail

curl localhost:9002/search -H "Content-Type: application/json" -d \
  '{"query":{"query":"", "from": "2024-01-01T15:00:00Z", "to": "2077-12-31T15:00:00Z"}, "size": 1000, "offset": 0}'

curl localhost:9002/search -H "Content-Type: application/json" -d \
  '{"query":{"query":"service:A", "from": "2024-01-01T15:00:00Z", "to": "2077-12-31T15:00:00Z"}, "size": 1000, "offset": 0}'

curl localhost:9002/search -H "Content-Type: application/json" -d \
  '{"query":{"query":"service:C", "from": "2024-01-01T15:00:00Z", "to": "2077-12-31T15:00:00Z"}, "size": 1000, "offset": 0, "with_total": true}'

# multi search example
curl "localhost:9002/complex-search" -H 'Content-Type: application/json' -d \
  '{
  "query":{"query":"service:hello", "from":"2024-01-01T15:00:00Z", "to":"2077-12-31T15:00:00Z"},
  "aggs":[{"field":"size_bytes", "group_by":"service", "func":"AGG_FUNC_SUM"}],
  "size": 1000, "offset": 0, "with_total": true,
  "hist": {"interval": "1m"}
  }'
