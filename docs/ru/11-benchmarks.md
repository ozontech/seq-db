---
id: benchmarks
---

# Бенчмарки

## Синтетические данные

### Методология

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

| Container     | Average Logs Per Second | Average Throughput | Average CPU Usage | Average RAM Usage |
|---------------|-------------------------|--------------------|-------------------|-------------------|
| seq-db        | 370_000                 | 48 MiB/s           | 3.3               | 1.8 GiB           |
| elasticsearch | 110_000                 | 14 MiB/s           | 1.9               | 2.4 GiB           |

Отсюда видно, что при сопоставимых значений используемых ресурсов, seq-db показала пропускную способность, которая в среднем 3.4 раз выше, чем пропускная способность у Elasticsearch.

### Результаты (read-path)
В оба хранилища был предварительно загружен одинаковый датасет (формат см выще). Тесты read-path запускались без 
нагрузки на запись. 

Важные замечания по настройкам запросов в elasticsearch: 
- был отключен кеш запросов (`request_cache=false`)
- был отключен подсчёт total hits (`track_total_hits=false`)

Тесты проводились при помощи утилиты grafana k6, параметры запросов указаны в каждом из сценариев, а также доступны в папке benchmarks/k6.

#### Сценарий 1: поиск всех логов с оффсетами
В Elasticsearch в конфигурации по умолчанию ограничивает `page_size*offset <= 10.000`.

Параметры: запросы параллельно с 20 потоков в течение 10 секунд. 
Выбирается и подгружается случайная страница [1–50].

| DB            | Avg      | P50      | P95     |
|---------------|----------|----------|---------|
| seq-db        | 5.56ms   | 5.05ms   | 9.56ms  |
| elasticsearch | 6.06ms   | 5.11ms   | 11.8ms  |


### Сценарий `status: in(500,400,403)`
Параметры: 20 looping VUs for 10s

| DB            | Avg       | P50           | P95      |
|---------------|-----------|---------------|----------|
| seq-db        | 364.68ms  | 356.96ms      | 472.26ms |
| elasticsearch | 21.68ms   | 18.91ms       | 29.84ms  |


### Сценарий `request: GET /english/images/top_stories.gif HTTP/1.0`
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


## Реальные (production) данные

### Методология

Помимо синтетических тестов, нам также необходимо было проверить, как seq-db и Elasticsearch показывают себя на реальных данных - логах, которые собираются со всех наших production сервисов.
Мы подготовили несколько сценариев бенчмарков, которые показывают перформанс характеристики в рамках одного инстанса и в рамках среднего по размеру по кластера:
- Тестирование пропускной способности одного инстанса seq-db и Elasticsearch (Конфигурация `1x1`);
- Тестирование пропускной способности 6 инстансов seq-db и Elasticsearch с RF=2 (Конфигурация `6x6`);

Также для всех тестов мы заранее подготовили датасеты, которые были собраны из production логов, и произвели над ними все необходимые [трансформации](https://ozontech.github.io/file.d/#/plugin/action/), чтобы минимизировать потребление CPU при непосредственной транспортировки логов из file.d в `seq-db` или Elasticsearch. Суммарный размер датасета равен 280 GiB.

Таким образом мы получили:
- Детерминированность в рамках датасета;
- Избавление от внешних зависимостей для доставки логов до file.d (конкретно в нашем случае - Apache Kafka);


Верхнеуровнево схема записи выглядит следующим образом:
```
┌──────────────┐        ┌───────────┐  
│ ┌──────┐     │        │           │  
│ │ file ├──┐  │    ┌──►│  elastic  │  
│ └──────┘  │  │    │   │           │  
│ ┌─────────▼┐ │    │   └───────────┘  
│ │          ├─┼────┘   ┌───────────┐  
│ │  file.d  │ │        │           │  
│ │          ├─┼───────►│  seq-db   │  
│ └──────────┘ │        │           │  
└──────────────┘        └───────────┘  
```

Ниже представлена информация о конфигурации кластера:
| Container                  | CPU                                       | RAM           | Disk                            |
|----------------------------|-------------------------------------------|---------------|---------------------------------|
| seq-db `(--mode store)`    | Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz | DDR4 3200 MHz | RAID 10 4x SSD MZILT7T6HALA/007 |
| seq-db `(--mode ingestor)` | Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz | DDR4 3200 MHz | -                               |
| elasticsearch (master)     | Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz | DDR4 3200 MHz | RAID 10 4x SSD MZILT7T6HALA/007 |
| elasticsearch (data)       | Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz | DDR4 3200 MHz | RAID 10 4x SSD MZILT7T6HALA/007 |
| file.d                     | Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz | DDR4 3200 MHz | -                               |

Далее мы выделили базовый набор полей, которые мы будем индексировать.
Так как Elasticsearch по умолчанию индексирует все поля, мы создали индекс `k8s-logs-index`, который индексирует только ранее выбранное нами множество полей.

#### Конфигурация `1x1`

В данной конфигурации использовались следующие настройки индексов:
```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": { "codec": "best_compression" },
    "number_of_replicas": 0,
    "number_of_shards": 6,
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "k8s_cluster": { "type": "keyword" },
      "k8s_container": { "type": "keyword" },
      "k8s_group": { "type": "keyword" },
      "k8s_label_jobid": { "type": "keyword" },
      "k8s_namespace": { "type": "keyword" },
      "k8s_node": { "type": "keyword" },
      "k8s_pod": { "type": "keyword" },
      "k8s_pod_label_cron": { "type": "keyword" },
      "client_ip": { "type": "keyword" },
      "http_code": { "type": "integer" },
      "http_method": { "type": "keyword" },
      "message": { "type": "text" }
    }
  }
}'
```

Ровно такую же конфигурацию индексирования и репликации мы задали и для seq-db.

##### Результат

| Container               | CPU Limit | RAM Limit | Average CPU Usage | Average RAM Usage |
|-------------------------|-----------|-----------|-------------------|-------------------|
| seq-db `(--mode store)` | 8         | 16 GiB    | 6.5               | 7 GiB             |
| seq-db `(--mode proxy)` | 8         | 8 GiB     | 7                 | 3 GiB             |
| elasticsearch (master)  | 2         | 4 GiB     | 0                 | 0 GiB             |
| elasticsearch (data)    | 16        | 32 GiB    | 15.8              | 30 GiB            |

| Container            | Average Throughput | Average Logs Per Second |
|----------------------|--------------------|-------------------------|
| seq-db               | 520 MiB/s          | 162_000                 |
| elasticsearch        | 195 MiB/s          | 62_000                  |

Отсюда видно, что при сопоставимых значений используемых ресурсов, seq-db показала пропускную способность, которая в среднем 2.6 раза выше, чем пропускная способность у Elasticsearch.

#### Конфигурация `6x6`

Для данной конфигурации было поднято 6 нод seq-db в режиме `--mode proxy` и 6 нод в режиме `--mode store`.

Настройки индексов сохранились такими же, за исключением `number_of_replicas=1`:
```bash
curl -X PUT "http://localhost:9200/k8s-logs-index/" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": { "codec": "best_compression" },
    "number_of_replicas": 1,
    "number_of_shards": 6,
  },
  "mappings": {
    "dynamic": "false",
    "properties": {
      "k8s_cluster": { "type": "keyword" },
      "k8s_container": { "type": "keyword" },
      "k8s_group": { "type": "keyword" },
      "k8s_label_jobid": { "type": "keyword" },
      "k8s_namespace": { "type": "keyword" },
      "k8s_node": { "type": "keyword" },
      "k8s_pod": { "type": "keyword" },
      "k8s_pod_label_cron": { "type": "keyword" },
      "client_ip": { "type": "keyword" },
      "http_code": { "type": "integer" },
      "http_method": { "type": "keyword" },
      "message": { "type": "text" }
    }
  }
}'
```


Также твикали index.merge.scheduler.max_thread_count, при уменьшении увеличивалась пропускная способность.
Ровно такую же конфигурацию индексирования и репликации мы задали и для seq-db.


##### Результат

| Container               | CPU Limit | RAM Limit | Replicas | Average CPU Usage (per instance) | Average RAM Usage (per instance) |
|-------------------------|-----------|-----------|----------|----------------------------------|----------------------------------|
| seq-db `(--mode proxy)` | 5         | 8 GiB     | 6        | 3.6                              | 1.5 GiB                          |
| seq-db `(--mode store)` | 8         | 16 GiB    | 6        | 6.1                              | 6.3 GiB                          |
| elasticsearch (data)    | 13        | 32 GiB    | 6        | 4.5                              | 13 GIB                           |

| Container            | Average Throughput | Average Logs Per Second | 
|----------------------|--------------------|-------------------------|
| seq-db               | 1.3 GiB/s          | 585139 docs/sec         |
| elasticsearch        | 113.58 MiB/s       | 37658 docs/sec          |
