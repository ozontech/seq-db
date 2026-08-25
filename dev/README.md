# dev/ — local debugging of seq-proxy + seq-db

Runs two processes (seq-db `store` + seq-proxy `proxy`) locally for debugging in
VS Code. A single `cmd/seq-db` binary runs in two modes — like
`quickstart/docker-compose.yaml`, but locally.

Controlled via `make` (targets are defined in `dev/Makefile`).

Processes start as a **plain binary, without headless-delve**. Debugging is done
through VS Code: the "Attach to Go process (pick)" config shows a list of
Go processes; you pick store or proxy — under the hood it's `dlv attach <PID>`.

## Ports

| Process | HTTP | gRPC (clients) | gRPC (internal) | debug/pprof |
|---------|------|----------------|-----------------|-------------|
| seq-db store | :9102 | — | **:9104** | :9201 |
| seq-proxy    | **:9002** | **:9004** | — | :9200 |

Clients talk to the proxy: `http://localhost:9002` / `grpc://localhost:9004`.
The proxy talks to the store at `localhost:9104`.

## Running

Commands can be run either from the `dev/` directory or from the repository root
via `make -C dev …` (the `-C` flag points make at the directory with the
`Makefile`):

```bash
make -C dev up        # build the debug binary and start store + proxy
make -C dev status    # show process status and PIDs
make -C dev logs      # tail both logs (or: make -C dev logs name=proxy)
make -C dev down      # stop
make -C dev restart   # down + up
```

Build only — `make -C dev build`.

Debugging requires `dlv` installed (VS Code attaches through it):
`go install github.com/go-delve/delve/cmd/dlv@latest`.

## Debugging in VS Code

1. `make -C dev up`
2. Set breakpoints in the code you need (store: `storeapi/`, `frac/`, `storage/`; proxy: `proxy/`, `proxyapi/`).
3. Run & Debug → **Attach to Go process (pick)**.
4. Pick a process from the list: store and proxy are distinguishable by name —
   `seq-db-store` and `seq-db-proxy` (the name is set via `exec -a`; it's one binary).

To debug both processes at once — open a second VS Code window and attach to the
other process.

> Since the processes are started without headless-delve, you can attach to them
> by PID without debugger conflicts (on macOS only one debugger can trace a
> process at a time — a headless setup would block local-attach).

## Files

- `Makefile` — start/stop/logs/status targets (`up`, `down`, `status`, `logs`, `build`, `restart`)
- `_helpers.sh` — shared bash functions for the Makefile recipes (paths via env)
- `config.store.yaml` / `config.proxy.yaml` — local configs (ports are separated)
- `data-store/` — store data (created automatically, safe to delete)
- `logs/{store,proxy}.log` — process logs
- `run/{store,proxy}.pid` — pid files

## Notes

- The binary is built with `-gcflags="all=-N -l"` (no optimizations/inlining) — required for the debugger.
- The mapping is a local copy in `dev/mappings.yaml` (mirrors `quickstart/mappings.yaml`). Config paths are relative to the process CWD, which the `up` recipe sets to `dev/` — so `make -C dev up` works from anywhere.
- The topology matches prod: proxy and store are separate processes. For simplified single-process debugging, the root `Makefile` has `make debug` (mode `single`).
- The `Makefile` targets GNU Make 3.81 (macOS): bash function bodies live in
  `_helpers.sh` and are loaded via `source`; the rest is written in recipes with
  `; \`-continuations (`.ONESHELL` is unsupported in 3.81).
