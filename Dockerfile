FROM gitlab-registry.***REMOVED***/docker/ubuntu-minbase:bionic

WORKDIR /seq-db

COPY ./bin/amd64/seq-db ./bin/amd64/distribution ./bin/amd64/analyzer ./

ENTRYPOINT ["./seq-db"]
