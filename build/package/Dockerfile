ARG APP_IMAGE=ubuntu:latest

# Build
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build

ARG VERSION
ARG BUILD_TIME
ARG TARGETARCH

WORKDIR /file.d

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ENV CGO_ENABLED 0
ENV GOOS linux
ENV GOARCH ${TARGETARCH:-amd64}

RUN go build -trimpath \
    -ldflags "-X github.com/ozontech/seqdb/buildinfo.Version=${VERSION}" \
    -ldflags "-X github.com/ozontech/seqdb/buildinfo.Version=${BUILD_TIME}" \
    -o seq-db ./cmd/seq-db

# Deploy
FROM $APP_IMAGE

WORKDIR /seq-db

COPY --from=build /seq-db/seq-db /seq-db/seq-db

CMD [ "./seq-db" ]
