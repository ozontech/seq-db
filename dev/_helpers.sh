#!/usr/bin/env bash
#
# dev/_helpers.sh — shared bash helpers for dev/Makefile.
#
# Not run on its own — only sourced from Makefile recipes. Reads paths from
# environment variables exported by the Makefile:
#   RUN — pid-file directory (dev/run)
#   LOG — process log directory (dev/logs)
#   BIN — path to the built debug binary
#
# Processes are started WITHOUT dlv — attach later via `dlv attach` from the IDE.
# exec -a overrides argv[0] (process name): store/proxy are distinguishable in
# `ps` and in the VS Code process list (one binary, different names).
#

set -euo pipefail

log() { printf '\033[1;34m[dev]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[dev]\033[0m %s\n' "$*" >&2; }

is_running() { [[ -f "$RUN/$1.pid" ]] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }

start_one() {
  local name="$1" cfg="$2" mode="$3"
  if is_running "$name"; then
    log "$name already running (pid $(cat "$RUN/$name.pid"))"
    return 0
  fi
  log "starting $name (mode=$mode)"
  mkdir -p "$RUN" "$LOG"
  exec -a "seq-db-$name" "$BIN" --mode="$mode" --config="$cfg" >"$LOG/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
}

wait_port() {
  local port="$1" name="$2" i=0
  while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
    i=$((i+1))
    if [[ $i -ge 60 ]]; then
      err "$name did not open port :$port within 60s. See $LOG/$name.log"
      tail -n 40 "$LOG/$name.log" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

stop_one() {
  local name="$1"
  if is_running "$name"; then
    local pid; pid="$(cat "$RUN/$name.pid")"
    log "stopping $name (pid $pid)"
    kill "$pid" 2>/dev/null || true
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$RUN/$name.pid"
}
