FROM gitlab-registry.***REMOVED***/docker/ubuntu-minbase:jammy

WORKDIR /seq-db

COPY ./bin/amd64/seq-db ./bin/amd64/distribution ./
COPY ./tests/data/mappings/logging-new.yaml ./default.yaml

ENTRYPOINT ["./seq-db"]
