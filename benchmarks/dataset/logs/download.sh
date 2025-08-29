#!/bin/bash

# Dataset is taken from here: https://github.com/elastic/rally-tracks/blob/master/http_logs/files.txt
docs=("documents-181998.json.bz2" "documents-191998.json.bz2" "documents-201998.json.bz2" "documents-211998.json.bz2" "documents-221998.json.bz2" "documents-231998.json.bz2" "documents-241998.json.bz2")
  
for doc in ${docs[@]}; do
  trimmed=$(basename ${doc} .json.bz2)
  if [ ! -f ${trimmed}.log ]; then
    curl http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/http_logs/${doc} | bzip2 --decompress -c > ${trimmed}.log
  fi
done
