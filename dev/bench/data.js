window.BENCHMARK_DATA = {
  "lastUpdate": 1758482653921,
  "repoUrl": "https://github.com/ozontech/seq-db",
  "entries": {
    "Benchmarks": [
      {
        "commit": {
          "author": {
            "name": "ozontech",
            "username": "ozontech"
          },
          "committer": {
            "name": "ozontech",
            "username": "ozontech"
          },
          "id": "103490203dca0185d380320121f9fdf31c8dd30c",
          "message": "ci: add continuous benchmarks",
          "timestamp": "2025-09-19T10:24:19Z",
          "url": "https://github.com/ozontech/seq-db/pull/144/commits/103490203dca0185d380320121f9fdf31c8dd30c"
        },
        "date": 1758482653496,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkBucketClean",
            "value": 42650,
            "unit": "ns/op",
            "extra": "28113 times\n4 procs"
          },
          {
            "name": "BenchmarkMutexListAppend",
            "value": 20832481,
            "unit": "ns/op\t 768.03 MB/s",
            "extra": "54 times\n4 procs"
          },
          {
            "name": "BenchmarkMutexListAppend - ns/op",
            "value": 20832481,
            "unit": "ns/op",
            "extra": "54 times\n4 procs"
          },
          {
            "name": "BenchmarkMutexListAppend - MB/s",
            "value": 768.03,
            "unit": "MB/s",
            "extra": "54 times\n4 procs"
          },
          {
            "name": "BenchmarkSeqListAppend",
            "value": 43305534,
            "unit": "ns/op\t 369.47 MB/s",
            "extra": "24 times\n4 procs"
          },
          {
            "name": "BenchmarkSeqListAppend - ns/op",
            "value": 43305534,
            "unit": "ns/op",
            "extra": "24 times\n4 procs"
          },
          {
            "name": "BenchmarkSeqListAppend - MB/s",
            "value": 369.47,
            "unit": "MB/s",
            "extra": "24 times\n4 procs"
          },
          {
            "name": "BenchmarkAggDeep",
            "value": 23.34,
            "unit": "ns/op",
            "extra": "53113066 times\n4 procs"
          },
          {
            "name": "BenchmarkAggWide",
            "value": 174.3,
            "unit": "ns/op",
            "extra": "8772456 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingNoSort",
            "value": 801641756,
            "unit": "ns/op\t49242192 B/op\t    5598 allocs/op",
            "extra": "2 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingNoSort - ns/op",
            "value": 801641756,
            "unit": "ns/op",
            "extra": "2 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingNoSort - B/op",
            "value": 49242192,
            "unit": "B/op",
            "extra": "2 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingNoSort - allocs/op",
            "value": 5598,
            "unit": "allocs/op",
            "extra": "2 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingWithSort",
            "value": 2515324757,
            "unit": "ns/op\t419283000 B/op\t   30232 allocs/op",
            "extra": "1 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingWithSort - ns/op",
            "value": 2515324757,
            "unit": "ns/op",
            "extra": "1 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingWithSort - B/op",
            "value": 419283000,
            "unit": "B/op",
            "extra": "1 times\n4 procs"
          },
          {
            "name": "Benchmark_SealingWithSort - allocs/op",
            "value": 30232,
            "unit": "allocs/op",
            "extra": "1 times\n4 procs"
          },
          {
            "name": "BenchmarkCopy",
            "value": 0.1683,
            "unit": "ns/op",
            "extra": "1000000000 times\n4 procs"
          },
          {
            "name": "BenchmarkIterate",
            "value": 0.3666,
            "unit": "ns/op",
            "extra": "1000000000 times\n4 procs"
          },
          {
            "name": "BenchmarkStatic",
            "value": 2.512,
            "unit": "ns/op",
            "extra": "477362289 times\n4 procs"
          },
          {
            "name": "BenchmarkNot",
            "value": 33.65,
            "unit": "ns/op",
            "extra": "34963804 times\n4 procs"
          },
          {
            "name": "BenchmarkNotEmpty",
            "value": 7.22,
            "unit": "ns/op",
            "extra": "166131271 times\n4 procs"
          },
          {
            "name": "BenchmarkOr",
            "value": 19.52,
            "unit": "ns/op",
            "extra": "63376788 times\n4 procs"
          },
          {
            "name": "BenchmarkAnd",
            "value": 16.86,
            "unit": "ns/op",
            "extra": "71237468 times\n4 procs"
          },
          {
            "name": "BenchmarkNAnd",
            "value": 17.37,
            "unit": "ns/op",
            "extra": "69231141 times\n4 procs"
          },
          {
            "name": "BenchmarkAndTree",
            "value": 83.06,
            "unit": "ns/op",
            "extra": "14457148 times\n4 procs"
          },
          {
            "name": "BenchmarkOrTree",
            "value": 171.4,
            "unit": "ns/op",
            "extra": "6674572 times\n4 procs"
          },
          {
            "name": "BenchmarkComplex",
            "value": 83.12,
            "unit": "ns/op",
            "extra": "14424555 times\n4 procs"
          },
          {
            "name": "BenchmarkParsing",
            "value": 1418,
            "unit": "ns/op",
            "extra": "829298 times\n4 procs"
          },
          {
            "name": "BenchmarkParsingLong",
            "value": 9996,
            "unit": "ns/op",
            "extra": "117638 times\n4 procs"
          },
          {
            "name": "BenchmarkSeqQLParsing",
            "value": 1083,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkSeqQLParsingLong",
            "value": 8981,
            "unit": "ns/op",
            "extra": "132175 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Deterministic/regular-cases-0",
            "value": 14.03,
            "unit": "ns/op",
            "extra": "81501319 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Deterministic/regular-cases-1",
            "value": 10.63,
            "unit": "ns/op",
            "extra": "100000000 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Deterministic/corner-cases-0",
            "value": 42.04,
            "unit": "ns/op",
            "extra": "24769042 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Deterministic/corner-cases-1",
            "value": 47.96,
            "unit": "ns/op",
            "extra": "25040098 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Deterministic/corner-cases-2",
            "value": 241.5,
            "unit": "ns/op",
            "extra": "4969818 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Deterministic/corner-cases-3",
            "value": 3186,
            "unit": "ns/op",
            "extra": "374986 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/tiny",
            "value": 21.5,
            "unit": "ns/op\t2977.09 MB/s",
            "extra": "55614740 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/tiny - ns/op",
            "value": 21.5,
            "unit": "ns/op",
            "extra": "55614740 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/tiny - MB/s",
            "value": 2977.09,
            "unit": "MB/s",
            "extra": "55614740 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/small",
            "value": 45.1,
            "unit": "ns/op\t5676.49 MB/s",
            "extra": "26657832 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/small - ns/op",
            "value": 45.1,
            "unit": "ns/op",
            "extra": "26657832 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/small - MB/s",
            "value": 5676.49,
            "unit": "MB/s",
            "extra": "26657832 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/medium",
            "value": 65.09,
            "unit": "ns/op\t15732.54 MB/s",
            "extra": "18486747 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/medium - ns/op",
            "value": 65.09,
            "unit": "ns/op",
            "extra": "18486747 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/medium - MB/s",
            "value": 15732.54,
            "unit": "MB/s",
            "extra": "18486747 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/large",
            "value": 800.3,
            "unit": "ns/op\t20471.13 MB/s",
            "extra": "1501923 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/large - ns/op",
            "value": 800.3,
            "unit": "ns/op",
            "extra": "1501923 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/large - MB/s",
            "value": 20471.13,
            "unit": "MB/s",
            "extra": "1501923 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/extra-large",
            "value": 90123,
            "unit": "ns/op\t11634.89 MB/s",
            "extra": "13288 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/extra-large - ns/op",
            "value": 90123,
            "unit": "ns/op",
            "extra": "13288 times\n4 procs"
          },
          {
            "name": "BenchmarkFindSequence_Random/extra-large - MB/s",
            "value": 11634.89,
            "unit": "MB/s",
            "extra": "13288 times\n4 procs"
          },
          {
            "name": "BenchmarkProcessDocuments",
            "value": 3426251,
            "unit": "ns/op\t 120.83 MB/s",
            "extra": "346 times\n4 procs"
          },
          {
            "name": "BenchmarkProcessDocuments - ns/op",
            "value": 3426251,
            "unit": "ns/op",
            "extra": "346 times\n4 procs"
          },
          {
            "name": "BenchmarkProcessDocuments - MB/s",
            "value": 120.83,
            "unit": "MB/s",
            "extra": "346 times\n4 procs"
          },
          {
            "name": "BenchmarkParseESTime/es_stdlib",
            "value": 163,
            "unit": "ns/op",
            "extra": "7431454 times\n4 procs"
          },
          {
            "name": "BenchmarkParseESTime/handwritten",
            "value": 33.04,
            "unit": "ns/op",
            "extra": "35909746 times\n4 procs"
          },
          {
            "name": "BenchmarkParseESTime/rfc3339",
            "value": 47.05,
            "unit": "ns/op",
            "extra": "25551000 times\n4 procs"
          },
          {
            "name": "BenchmarkESBulk",
            "value": 50971,
            "unit": "ns/op\t6317.38 MB/s\t     385 B/op\t       9 allocs/op",
            "extra": "23456 times\n4 procs"
          },
          {
            "name": "BenchmarkESBulk - ns/op",
            "value": 50971,
            "unit": "ns/op",
            "extra": "23456 times\n4 procs"
          },
          {
            "name": "BenchmarkESBulk - MB/s",
            "value": 6317.38,
            "unit": "MB/s",
            "extra": "23456 times\n4 procs"
          },
          {
            "name": "BenchmarkESBulk - B/op",
            "value": 385,
            "unit": "B/op",
            "extra": "23456 times\n4 procs"
          },
          {
            "name": "BenchmarkESBulk - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "23456 times\n4 procs"
          },
          {
            "name": "BenchmarkMergeQPRs_ReusingQPR",
            "value": 2233793,
            "unit": "ns/op",
            "extra": "523 times\n4 procs"
          },
          {
            "name": "BenchmarkRandomJSON",
            "value": 81426,
            "unit": "ns/op",
            "extra": "17786 times\n4 procs"
          },
          {
            "name": "BenchmarkRandomDoc",
            "value": 4660,
            "unit": "ns/op",
            "extra": "251413 times\n4 procs"
          },
          {
            "name": "BenchmarkGenerateDocs",
            "value": 1136,
            "unit": "ns/op",
            "extra": "957223 times\n4 procs"
          },
          {
            "name": "BenchmarkGenerateDocsJSON",
            "value": 1759,
            "unit": "ns/op",
            "extra": "672789 times\n4 procs"
          },
          {
            "name": "BenchmarkGenerateDocsJSONFields",
            "value": 3285,
            "unit": "ns/op",
            "extra": "358015 times\n4 procs"
          },
          {
            "name": "BenchmarkBitmask",
            "value": 191726524,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          }
        ]
      }
    ]
  }
}