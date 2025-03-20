---
id: log-storage-comparison
---

# Log storage comparison

## Database features

| DB                          | seq-db      | Elasticsearch | Loki        | VictoriaLogs |
|-----------------------------|-------------|---------------|-------------|--------------|
| Record updates              | &cross;     | &check;       | &cross;     | &cross;      |
| Record deletion             | &cross;     | &check;       | &check; [1] | &cross;      |
| Retention by time           | &cross;     | &check;       | &check;     | &check;      |
| Retention by size           | &check;     | &check;       | &cross;     | &check;      |
| Retention per stream/tenant | &cross;     | &cross;       | &check;     | &cross;      |
| Multitenancy Auth           | &cross;     | &cross;       | &cross;     | &cross;      |
| HA setup                    | &check;     | &check;       | &check;     | &check;      |
| Replication                 | &check;     | &check;       | &check;     | &cross;      |
| Shards                      | &check;     | &check;       | &check;     | &cross;      |
| Hot-Cold                    | &check;     | &check;       | &cross;     | &cross;      |
| Schemaless                  | &check; [2] | &check; [3]   | &check;     | &check;      |
| Full-text search            | &check;     | &check;       | &check;     | &check;      |
| Full-scan                   | &cross;     | &cross;       | &check;     | &check;      |
| Migration tool              | &cross;     | &cross;       | &cross;     | &cross;      |

[1] – Loki can delete log entries in background using compaction.

[2] – seq-db is schemaless, but requires fields mapping like Elasticsearch, but without preserving information about the
data type.

[3] – Elasticsearch can accept data without explicitly describing the schema.
However, after the first type definition, you cannot change them without reindexing.

## Log collectors support

| DB                       | seq-db      | Elasticsearch | Loki    | VictoriaLogs |
|--------------------------|-------------|---------------|---------|--------------|
| file.d                   | &check;     | &check;       | &cross; | &check;      |
| Vector                   | &cross; [1] | &check;       | &check; | &check;      |
| Filebeat                 | &cross; [1] | &check;       | &cross; | &check;      |
| Fluentbit                | &cross; [1] | &check;       | &check; | &check;      |
| Fluentd                  | &cross; [1] | &check;       | &check; | &check;      |
| Logstash                 | &cross; [1] | &check;       | &check; | &check;      |
| Syslog/Rsyslog/Syslog-ng | &cross; [1] | &check;       | &cross; | &check;      |
| Alloy                    | &cross; [1] | &cross;       | &check; | &check;      |
| Telegraf                 | &cross; [1] | &check;       | &check; | &check;      |
| OpenTelemetry Collector  | &cross; [1] | &check;       | &check; | &check;      |
| Journald                 | &cross; [1] | &check;       | &cross; | &check;      |
| DataDog                  | &cross; [1] | &check;       | &cross; | &check;      |

[1] – seq-db supports ES _bulk protocol, but it is not tested with these log collectors.
