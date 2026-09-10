#!/usr/bin/env bash
# Export merged seq-db profiles from Pyroscope for a time window as standard
# pprof files, so perf-analyzer can use `go tool pprof`. Wraps `profilecli`
# (shipped in the Pyroscope image) with the --user/--network flags it needs.
#
# usage: collect-profiles.sh <out_dir> <from_epoch> <to_epoch> [service]
#   e.g. collect-profiles.sh .perf-runs/001-fix 1788976537 1788976666
set -uo pipefail

OUT="${1:?usage: collect-profiles.sh <out_dir> <from_epoch> <to_epoch> [service]}"
FROM="${2:?missing from_epoch}"
TO="${3:?missing to_epoch}"
SERVICE="${4:-seq-db}"
PYRO_IMAGE="${PYRO_IMAGE:-grafana/pyroscope:2.3.1}"

mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
UID_GID="$(id -u):$(id -g)"

pyro() { # $1 = pyroscope profile-type, $2 = output filename
  if docker run --rm --network host --user "$UID_GID" -v "$OUT:/out" \
      --entrypoint /usr/bin/profilecli "$PYRO_IMAGE" \
      query profile --url=http://localhost:4040 \
      --query="{service_name=\"$SERVICE\"}" --profile-type="$1" \
      --from="$FROM" --to="$TO" --output="pprof=/out/$2" -f >/dev/null 2>&1; then
    echo "  $2"
  else
    echo "  $2 FAILED (no samples in [$FROM,$TO]? stack down?)"
  fi
}

echo "exporting profiles for service=$SERVICE window=[$FROM,$TO] -> $OUT"
pyro 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'  cpu.pprof
pyro 'memory:alloc_space:bytes:space:bytes'         allocs.pprof
pyro 'memory:inuse_space:bytes:space:bytes'         heap.pprof
pyro 'goroutine:goroutine:count:goroutine:count'    goroutine.pprof
