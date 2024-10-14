#!/bin/bash

#set -euxo pipefail

curl localhost:9002/fetch -H "Content-Type: application/json" -d \
  '{"ids":["974bb71590010000-810259505a231802", "bd05705a90010000-4b03750dffe73853", "cd42eb5e90010000-3f008745f16d0836", "xxx"]}'
