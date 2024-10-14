#!/bin/bash

set -euxo pipefail

# histogram example
curl "localhost:9002/histogram" -H 'Content-Type: application/json' -d \
  '{
  "query":{"query":"service:hello", "from":"2024-01-01T15:00:00Z", "to":"2077-12-31T15:00:00Z"},
  "hist": {"interval": "1m"}
  }'
