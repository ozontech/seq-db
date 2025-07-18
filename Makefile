SHELL := /bin/bash

OS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH ?= $(shell uname -m | tr '[:upper:]' '[:lower:]')

ifeq ($(ARCH), x86_64)
ARCH := amd64
endif

VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)
TIME := $(shell date '+%Y-%m-%d_%H:%M:%S')

LOCAL_BIN:=$(CURDIR)/bin
.PHONY: build-binaries
build-binaries:
	CGO_ENABLED=0 GOOS=${OS} GOARCH=${ARCH} go build \
      -trimpath \
      -ldflags "-X github.com/ozontech/seq-db/buildinfo.Version=${VERSION} \
                -X github.com/ozontech/seq-db/buildinfo.BuildTime=${TIME}" \
      -o ./bin/${OS}-${ARCH}/ \
      ./cmd/...

.PHONY: build-image
build-image:
	docker buildx build --platform linux/amd64 \
		--build-arg VERSION=${VERSION} \
		--build-arg BUILD_TIME=${TIME} \
		--file build/package/Dockerfile \
		-t ghcr.io/ozontech/seq-db:${VERSION} \
		.

.PHONY: run
run: build-binaries
	@$(eval DATA_DIR := $(shell mktemp -d))
	${LOCAL_BIN}/${OS}-${ARCH}/seq-db \
		--mode=single \
		--mapping=auto \
		--data-dir=${DATA_DIR}

.PHONY: push-image
push-image: build-image
	docker push ghcr.io/ozontech/seq-db:${VERSION}

.PHONY: test
test:
	go test ./... -count 1

.bin-deps: export GOBIN := $(LOCAL_BIN)
.bin-deps:
	$(info Installing binary dependencies...)

	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.26.1
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.26.1
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.1-0.20240319094008-0393e58bdf10

.PHONY: bin-deps
bin-deps: .bin-deps

.PHONY: proto
proto: bin-deps
proto:
	protoc \
		-I=api \
		-I=vendorpb \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--plugin=protoc-gen-grpc-gateway=$(LOCAL_BIN)/protoc-gen-grpc-gateway \
		--plugin=protoc-gen-go-vtproto=$(LOCAL_BIN)/protoc-gen-go-vtproto \
		--go_out=pkg --go_opt=paths=source_relative \
		--grpc-gateway_out=pkg --grpc-gateway_opt=paths=source_relative --grpc-gateway_opt=generate_unbound_methods=true \
		--go-vtproto_out=pkg --go-vtproto_opt=paths=source_relative \
		--go-vtproto_opt=features=all \
		$(shell find api -name '*.proto')

# arg -count=1 is used to disable tests caching (it is necessary when we want reproduce bugs of broken test isolation)
.PHONY: ci-tests
ci-tests:
	set -o pipefail ;\
	go test -v -short -count=1 -coverpkg=github.com/ozontech/seq-db/... -covermode=atomic -coverprofile=cover-tmp.out ./... 2>&1 | \
		tee /dev/stderr | go-junit-report -set-exit-code > junit.xml
	grep -vE ".pb.go|pb.*.go" cover-tmp.out > cover.out || cp cover-tmp.out cover.out
	go tool cover -func=./cover.out

.PHONY: ci-tests-race
ci-tests-race:
	set -o pipefail ;\
	go test -short ./... -count=1 -race

# run diff lint like in pipeline
.lint:
	$(info Running lint...)
	GOBIN=$(LOCAL_BIN) go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6 run \
		--config=.golangci.yaml ./...

.PHONY: lint
lint: .lint

.PHONY: imports
imports:
	find . \
	-path './pkg' -prune -o \
	-type f -name '*.go' \
	! -name '*.pb.go' \
	! -name '*.pb.gw.go' \
	! -name '*_mock.go' \
	-exec goimports -w -local 'github.com/ozontech/seq-db' {} +

.PHONY: mock
mock:
	go run github.com/golang/mock/mockgen@latest \
		-source=proxyapi/grpc_v1.go \
		-destination=proxyapi/mock/grpc_v1.go \
		-package mock
	go run github.com/golang/mock/mockgen@latest \
		-destination=proxy/search/mock/store_api_client_mock.go \
		-package mock \
		github.com/ozontech/seq-db/pkg/storeapi StoreApiClient

get-version:
	@echo ${VERSION}

LOCALE ?= 'en' # Use 'en' or 'ru'.
DOCS_VERSION := v0.0.3

.PHONY: serve-docs
serve-docs:
	docker run -e LOCALE=$(LOCALE) --rm -it -p 3000:3000 \
		-v ./docs/en:/website/docs/seq-db \
		-v ./docs/ru:/website/i18n/ru/docusaurus-plugin-content-docs/current/seq-db \
		ghcr.io/ozontech/seq-db-docs:$(DOCS_VERSION)

.PHONY: build-docs
build-docs:
	docker run --rm \
		-v ./bin/docs:/website/build \
 		-v ./docs/en:/website/docs/seq-db \
		-v ./docs/ru:/website/i18n/ru/docusaurus-plugin-content-docs/current/seq-db \
		--entrypoint /bin/sh \
		ghcr.io/ozontech/seq-db-docs:$(DOCS_VERSION) \
		-c 'npm run build'
