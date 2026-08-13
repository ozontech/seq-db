#!/usr/bin/env bash
set -euo pipefail

sync
echo 3 | tee /proc/sys/vm/drop_caches >/dev/null
