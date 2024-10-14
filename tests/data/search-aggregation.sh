#!/bin/bash

set -euxo pipefail

# aggregation example
curl "localhost:9002/aggregate" -H 'Content-Type: application/json' -d \
  '{
  "query":{"query":"service:hello", "from":"2024-01-01T15:00:00Z", "to":"2077-12-31T15:00:00Z"},
  "aggs":[
      { "field":"size_bytes", "group_by":"service", "func":"AGG_FUNC_QUANTILE", "quantiles": [0.5, 0.9, 0.95, 0.99] }
    ]
  }'
