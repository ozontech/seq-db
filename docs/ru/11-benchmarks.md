---
id: benchmarks
---

# Бенчмарки

## Методология

Мы подготовили набор бенчмарков, которые постарались сделать максимально воспроизводимыми и детерминированными.
Датасет, который использовался во время бенчмарков, детерминирован и состоит из 40 гигабайт (219 млн) структурированных логов в формате json.

Пример лога: 
```json
{
    "@timestamp": 897484581,
    "clientip": "162.146.3.0",
    "request": "GET /images/logo_cfo.gif HTTP/1.1",
    "status": 200,
    "size": 1504
}
```

## Local Deploy

Тесты запускались на AWS хосте `c6a.4xlarge`, который имеет следующую конфигурацию:

| CPU                                      | RAM    | Disk |
|------------------------------------------|--------|------|
| AMD EPYC 7R13 Processor 3.6 GHz, 16 vCPU | 32 GiB | GP3  |

Более подробно ознакомиться с тем, какие компоненты участвую в данном сьюте, какие настройки компонентов были выставлены и как его запускать, вы можете [тут](https://github.com/ozontech/seq-db/tree/master/benchmarks).

Ниже представлена информация о конфигурации локального кластера:

| Container                | Replicas | CPU Limit | RAM Limit |
|--------------------------|----------|-----------|-----------|
| seq-db `(--mode single)` | 1        | 4         | 8 GiB     |
| elasticsearch            | 1        | 4         | 8 GiB     |
| file.d                   | 1        | -         | 12 GiB    |

### Результаты (write-path)

На синтетических тестах у нас получились следующие числа:

| Container     | Avg. Logs/sec | Avg. Throughput | Avg. CPU Usage | Avg. RAM Usage |
|---------------|---------------|-----------------|----------------|----------------|
| seq‑db        | 370,000       | 48MiB/s         | 3.3vCPU        | 1.8GiB         |
| elasticsearch | 110,000       | 14MiB/s         | 1.9vCPU        | 2.4GiB         |

Отсюда видно, что при сопоставимых значений используемых ресурсов, seq-db показала пропускную способность, которая в среднем 3.4 раз выше, чем пропускная способность у Elasticsearch.

### Результаты (read-path)
В оба хранилища был предварительно загружен одинаковый датасет (формат см выще). Тесты read-path запускались без 
нагрузки на запись. 

Важные замечания по настройкам запросов в elasticsearch: 
- был отключен кеш запросов (`request_cache=false`)
- был отключен подсчёт total hits (`track_total_hits=false`)

Тесты проводились при помощи утилиты [Grafana k6](https://k6.io/), параметры запросов указаны в каждом из сценариев, а также доступны в папке benchmarks/k6.

#### Сценарий: поиск всех логов с оффсетами
В Elasticsearch в конфигурации по умолчанию ограничивает `page_size*offset <= 10.000`.

Параметры: запросы параллельно с 20 потоков в течение 10 секунд. 
Выбирается и подгружается случайная страница [1–50].

| DB            | Avg      | P50      | P95     |
|---------------|----------|----------|---------|
| seq-db        | 5.56ms   | 5.05ms   | 9.56ms  |
| elasticsearch | 6.06ms   | 5.11ms   | 11.8ms  |


#### Сценарий `status: in(500,400,403)`
Параметры: 20 looping VUs for 10s

| DB            | Avg       | P50           | P95      |
|---------------|-----------|---------------|----------|
| seq-db        | 364.68ms  | 356.96ms      | 472.26ms |
| elasticsearch | 21.68ms   | 18.91ms       | 29.84ms  |


#### Сценарий `request: GET /english/images/top_stories.gif HTTP/1.0`
Параметры: 20 looping VUs for 10s

| DB            | Avg      | P50      | P95      |
|---------------|----------|----------|----------|
| seq-db        | 269.98ms | 213.43ms | 704.19ms |
| elasticsearch | 46.65ms  | 43.27ms  | 80.53ms  |



#### Сценарий: агрегация с подсчётом кол-ва логов с определённым статусом
Написан запрос c таким sql аналогом: `SELECT status, COUNT(*) GROUP BY status`.

Параметры: 10 запросов параллельно с 2 потоков.

| DB            | Avg    | P50    | P95    |
|---------------|--------|--------|--------|
| seq-db        | 16.81s | 16.88s | 16.10s |
| elasticsearch | 6.46s  | 6.44s  | 6.57s  |


#### Сценарий: минимальный размер лога каждого статуса
SQL-аналог: `SELECT status, MIN(size) GROUP BY status`.


Параметры: 5 итераций с 1 потока. 

| DB            | Avg     | P50     | P95     |
|---------------|---------|---------|---------|
| seq-db        | 33.34s  | 33.41s  | 33.93s  |
| elasticsearch | 16.88s  | 16.82s  | 17.5s   |



#### Сценарий : range запросы - выборка из 5000 документов 
Параметры: 20 потоков, 10 секунд.

Выбирается случайная страница [1-50], на каждой странице по 100 документов.

| DB            | Avg      | P50      | P95      |
|---------------|----------|----------|----------|
| seq-db        | 406.09ms | 385.13ms | 509.05ms |
| elasticsearch | 22.75ms  | 18.06ms  | 64.61ms  |


## K8S Deploy

Ниже представлена информация о конфигурации кластера:

| Container                   | CPU                       | RAM          | Disk          |
|-----------------------------|---------------------------|--------------|---------------|
| seq‑db (`--mode store`)     | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | RAID10, 4×SSD |
| seq‑db (`--mode ingestor`)  | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | –             |
| elasticsearch (master/data) | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | RAID10, 4×SSD |
| file.d                      | Xeon Gold 6240R @ 2.40GHz | DDR4 3200MHz | –             |

Далее мы выделили базовый набор полей, которые мы будем индексировать.
Так как Elasticsearch по умолчанию индексирует все поля, мы создали индекс `k8s-logs-index`, который индексирует только ранее выбранное нами множество полей.

### Конфигурация `1x1`

В данной конфигурации использовались следующие настройки индексов:

```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "number_of_shards": "6",
      "refresh_interval": "1s",
      "number_of_replicas": "0",
      "codec": "best_compression",
      "merge.scheduler.max_thread_count": "2",
      "translog": { "durability": "request" }
    }
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "request": { "type": "text" },
      "size": { "type": "keyword" },
      "status": { "type": "keyword" },
      "clientip": { "type": "keyword" }
    }
  }
}'
```

Ровно такую же конфигурацию индексирования и репликации мы задали и для seq-db.

#### Результат

| Container               | CPU Limit | RAM Limit | Avg. CPU | Avg. RAM |
|-------------------------|-----------|-----------|----------|----------|
| seq‑db (`--mode store`) | 10        | 16GiB     | 8.81     | 3.2GB    |
| seq‑db (`--mode proxy`) | 6         | 8GiB      | 4.92     | 4.9 GiB  |
| elasticsearch (data)    | 16        | 32GiB     | 15.18    | 13GB     |

| Container     | Avg. Throughput | Logs/sec  |
|---------------|-----------------|-----------|
| seq‑db        | 181MiB/s        | 1,403,514 |
| elasticsearch | 61MiB/s         | 442,924   |

Отсюда видно, что при сопоставимых значений используемых ресурсов, seq-db показала пропускную способность, которая в среднем ~2.9 раза выше, чем пропускная способность у Elasticsearch.

### Конфигурация `6x6`

Для данной конфигурации было поднято 6 нод seq-db в режиме `--mode proxy` и 6 нод в режиме `--mode store`.

Настройки индексов сохранились такими же, за исключением `number_of_replicas=1`:

```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "number_of_shards": "6",
      "refresh_interval": "1s",
      "number_of_replicas": "1",
      "codec": "best_compression",
      "merge.scheduler.max_thread_count": "2",
      "translog": { "durability": "request" }
    }
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "request": { "type": "text" },
      "size": { "type": "keyword" },
      "status": { "type": "keyword" },
      "clientip": { "type": "keyword" }
    }
  }
}'
```

#### Results

| Container                | CPU Limit | RAM Limit | Replicas | Avg. CPU (per instance) | Avg. RAM (per instance) |
|--------------------------|-----------|-----------|----------|-------------------------|-------------------------|
| seq‑db (`--mode proxy`)  | 3         | 8GiB      | 6        | 1.87                    | 2.2GiB                  |
| seq‑db (`--mode store`)  | 10        | 16GiB     | 6        | 7.40                    | 2.5GiB                  |
| elasticsearch (data)     | 13        | 32GiB     | 6        | 7.34                    | 8.8GiB                  |

| Container     | Avg. Throughput | Avg. Logs/sec  |
|---------------|-----------------|----------------|
| seq‑db        | 436MiB/s        | 3,383,724      |
| elasticsearch | 62MiB/s         | 482,596        |
