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
    -o ./ ./cmd/...

# Deploy
FROM $APP_IMAGE

WORKDIR /seq-db

COPY --from=build /seq-db/seq-db /seq-db/seq-db
COPY --from=build /seq-db/unpacker /seq-db/unpacker
COPY --from=build /seq-db/inserter /seq-db/inserter

ENTRYPOINT [ "./seq-db" ]
