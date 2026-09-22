ARG GO_VERSION
ARG APP_IMAGE=ubuntu:latest

# Build
FROM golang:${GO_VERSION:-1.25} AS build

WORKDIR /seq-db

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ARG VERSION
ARG BUILD_TIME

ARG CGO_ENABLED=1

RUN CGO_ENABLED=${CGO_ENABLED} go build -trimpath \
    -tags 'netgo osusergo' \
    -ldflags "-w -s  \
    -X github.com/ozontech/seq-db/buildinfo.Version=${VERSION}  \
    -X github.com/ozontech/seq-db/buildinfo.BuildTime=${BUILD_TIME}" \
    -o seq-db ./cmd/seq-db

# Deploy
FROM $APP_IMAGE

# Run as a non-privileged user (CIS Docker Benchmark 4.1).
ARG SEQ_DB_UID=1001
ARG SEQ_DB_GID=1001

RUN groupadd --gid ${SEQ_DB_GID} seq-db \
    && useradd --uid ${SEQ_DB_UID} --gid ${SEQ_DB_GID} --shell /usr/sbin/nologin seq-db

WORKDIR /seq-db

COPY --from=build /seq-db/seq-db /seq-db/seq-db

RUN mkdir -p /seq-db/data /seq-db/logs \
    && chown -R seq-db:seq-db /seq-db

USER seq-db

ENTRYPOINT [ "./seq-db" ]
