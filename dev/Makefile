# dev/Makefile — local launch of seq-db (store) + seq-proxy.
#
# A Makefile equivalent of dev/dev.sh (the original sh script is kept unchanged).
# Processes run as a PLAIN binary (no headless-delve); debugging is done via
# VS Code: "Attach to Go process (pick)" lists store/proxy (dlv attach).
#
#   make up            build the debug binary and start store + proxy
#   make down          stop
#   make status        show process status and PIDs
#   make logs          tail both logs (or: make logs name=proxy)
#   make build         build the debug binary only
#   make restart       down + up
#
# Run from dev/ or via `make -C dev <target>` from the repo root.
#
# GNU Make 3.81 (macOS) without .ONESHELL passes each recipe line to a separate
# shell, but a backslash+newline joins lines into one. So bash function bodies
# (which need real newlines) live in dev/_helpers.sh and are loaded via `source`;
# everything linear is written straight in the recipe with `; \`.
#
SHELL := /bin/bash

DEV  := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ROOT := $(abspath $(DEV)/..)
RUN  := $(DEV)/run
LOG  := $(DEV)/logs
DATA := $(DEV)/data-store

OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m | tr '[:upper:]' '[:lower:]')
ifeq ($(ARCH), x86_64)
ARCH := amd64
endif
ifeq ($(ARCH), aarch64)
ARCH := arm64
endif
BIN := $(ROOT)/bin/$(OS)-$(ARCH)/seq-db

STORE_CFG := $(DEV)/config.store.yaml
PROXY_CFG := $(DEV)/config.proxy.yaml
STORE_GRPC_PORT := 9104

# Export paths into the environment — the functions in _helpers.sh read them.
export DEV ROOT RUN LOG BIN

name ?=

.DEFAULT_GOAL := help

.PHONY: help build up down status logs restart

help:
	@echo "usage: make {up|down|status|logs [name=store|proxy]|build|restart}"

build:
	@source "$$DEV/_helpers.sh"; \
	log "building debug binary -> $(BIN)"; \
	cd "$(ROOT)"; \
	CGO_ENABLED=0 go build -gcflags="all=-N -l" -o "$(BIN)" ./cmd/seq-db

up: build
	@source "$$DEV/_helpers.sh"; \
	cd "$(DEV)"; \
	mkdir -p "$(DATA)" "$(RUN)" "$(LOG)"; \
	start_one store "$(STORE_CFG)" store; \
	wait_port "$(STORE_GRPC_PORT)" store; \
	start_one proxy "$(PROXY_CFG)" proxy; \
	log "ready. Debug via VS Code: \"Attach to Go process (pick)\"."; \
	log "  store  pid=$$(cat "$(RUN)/store.pid")  grpc=:$(STORE_GRPC_PORT)  log=$(LOG)/store.log"; \
	log "  proxy  pid=$$(cat "$(RUN)/proxy.pid")  http=:9002 grpc=:9004   log=$(LOG)/proxy.log"; \
	log "  clients -> http://localhost:9002  grpc://localhost:9004"; \
	log "  stop: make down"

down:
	@source "$$DEV/_helpers.sh"; \
	stop_one proxy; \
	stop_one store; \
	log "stopped."

status:
	@source "$$DEV/_helpers.sh"; \
	for name in store proxy; do \
	  if is_running "$$name"; then \
	    log "$$name: running (pid $$(cat "$(RUN)/$$name.pid"))"; \
	  else \
	    log "$$name: stopped"; \
	  fi; \
	done

logs:
	@source "$$DEV/_helpers.sh"; \
	if [[ -z "$(name)" ]]; then \
	  tail -n 200 "$(LOG)/store.log" "$(LOG)/proxy.log" 2>/dev/null || err "no logs"; \
	else \
	  tail -n 200 -f "$(LOG)/$(name).log"; \
	fi

restart: down up
	@:
