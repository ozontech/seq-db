# Публичное API

## Введение

seq-db состоит из двух компонентов - proxy и store:

- seq-db proxy и seq-db store взаимодействуют через
  *внутреннее* [seq-db store gRPC API](https://github.com/ozontech/seq-db/tree/main/api/storeapi).
- Поиск логов происходит при помощи [seq-db proxy gRPC API](https://github.com/ozontech/seq-db/tree/main/api/seqproxyapi/v1).
- Прием логов происходит при помощи [Elasticsearch совместимого HTTP API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
- Для целей отладки seq-db proxy имеет `HTTP API`, похожее на `gRPC API`

В этом документе описаны `seq-db ingestion API` и `seq-db search API`

## Bulk HTTP API

seq-db совместима с Elasticsearch bulk API

### `/`

Возвращает захардкоженный ответ, указывающий на версию используемого Elasticsearch. Это позволяет использовать seq-db
в качестве Elasticsearch вывода для logstash, filebeat и других инструментов доставки логов.

Пример запроса:

```bash
curl -X GET http://localhost:9002/
```

Пример успешного ответа:

```json
{
  "cluster_name": "seq-db",
  "version": {
    "number": "8.9.0"
  }
}
```

### `/_bulk`

Принимает тело запроса, разбирает его на документы и метаданные, затем записывает в хранилища через внутренний API.

Пример запроса:

```bash
curl -X POST http://localhost:9002/_bulk -d '
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "5", "time": "2024-12-23T18:00:36.357Z"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "6"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "7"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "8"}
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "9"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "10"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "11"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "12"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "13"}
{"index":""}
{"k8s_pod":"seq-db", "request_time": "14"}
'
```

Пример успешного ответа:

```json
{
  "took": 11,
  "errors": false,
  "items": [
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } },
    { "create": { "status": 201 } }
  ]
}
```

Можно заметить, что в поле `index` нет данных: seq-db игнорирует это поле, потому что использует маппинг для индексации полей.
Больше о маппинге и индексах можно прочитать в [документации](03-index-types.md)

Пример ответа при превышении лимита запросов:

Не должно быть более `consts.IngestorMaxInflightBulks` (по умолчанию 32) одновременных запросов, в противном случае  
запрос будет ограничен по частоте, и seq-db ответит кодом ответа `429`.

Пример ответа с ошибкой:

В случае ошибки, seq-db вернет код ответа `500` и сообщение об ошибке.
Например, при попытке вставить некорректный json:

```bash
curl -v -X POST http://localhost:9002/_bulk -d '
{"index":""}
{"k8s_pod":"seq-proxy", "request_time": "123}
'
```

мы получим

```text
processing doc: unexpected end of string near `", "request_time": "123`
                                                  ^
```

## Search gRPC API

### `/Search`

Метод поиска документов. Принимает поисковый запрос в формате seq-ql и возвращает документы, удовлетворяющие поисковому запросу.

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z",
    "query": "k8s_pod:seq-db"
  },
  "size": 2,
  "with_total": true
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/Search
```

Пример успешного ответа:

```json
{
  "total": "5",
  "docs": [
    {
      "id": "0593adf493010000-d901eee224290dc6",
      "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMyJ9",
      "time": "2024-12-23T18:00:36.357Z"
    },
    {
      "id": "0593adf493010000-d9013865e424dba1",
      "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMSJ9",
      "time": "2024-12-23T18:00:36.357Z"
    }
  ],
  "error": {
    "code": "ERROR_CODE_NO"
  }
}
```

Поле `data` содержит оригинальный документ, закодированный в base64. Если декодировать его,

```bash
echo 'eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMyJ9' | base64 -d | jq
```

то получится

```json
{
  "k8s_pod": "seq-db",
  "request_time": "13"
}
```

### `/GetAggregation`

Агрегации позволяют вычислять статистические значения (сумма, среднее, максимум, минимум, квантиль, уникальность, количество) по
полям документов, соответствующим запросу. Также агрегации поддерживают вычисление статистических значений среди различных временных интервалов (также известные как таймсерии).

Агрегации можно вызывать двумя способами:

- через отдельный gRPC обработчик: [`GetAggregation`](#getaggregation)
- вместе с поиском и гистограммами: [`ComplexSearch`](#complexsearch)

> В примерах используется API `GetAggregation`, которая по структуре запроса и ответа совпадает с `ComplexSearch`.

Поддерживаемые функции агрегации:

- `AGG_FUNC_SUM` — сумма значений поля
- `AGG_FUNC_AVG` — среднее значение поля
- `AGG_FUNC_MIN` — минимальное значение поля
- `AGG_FUNC_MAX` — максимальное значение поля
- `AGG_FUNC_QUANTILE` — вычисление квантилей для поля
- `AGG_FUNC_UNIQUE` — вычисление уникальных значений поля (не поддерживается для вычисления таймсерий)
- `AGG_FUNC_COUNT` — подсчёт количества документов по группе

#### Примеры агрегаций

Исходные документы:

```json lines
{"service": "svc1", "latency": 100}
{"service": "svc1", "latency": 300}
{"service": "svc2", "latency": 400}
{"service": "svc2", "latency": 200}
{"service": "svc3", "latency": 500}
```

##### Вычисление SUM, AVG, MIN, MAX

**Запрос:**

```sh
grpcurl -plaintext -d '
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "field": "latency",
      "func": "AGG_FUNC_SUM"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_AVG"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MIN"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MAX"
    }
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Ответ:**

```json
{
  "aggs": [
    {
      "buckets": [
        {"value": 1500}
      ]
    },
    {
      "buckets": [
        {"value": 300}
      ]
    },
    {
      "buckets": [
        {"value": 100}
      ]
    },
    {
      "buckets": [
        {"value": 500}
      ]
    }
  ]
}
```

##### Вычисление SUM, AVG, MIN, MAX c group_by

**Запрос:**

```sh
grpcurl -plaintext -d '
{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "field": "latency",
      "func": "AGG_FUNC_SUM",
      "group_by": "service"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_AVG",
      "group_by": "service"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MIN",
      "group_by": "service"
    },
    {
      "field": "latency",
      "func": "AGG_FUNC_MAX",
      "group_by": "service"
    }
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Ответ:**

```json
{
  "aggs": [
    {
      "buckets": [
        {"key": "svc2", "value": 600},
        {"key": "svc3", "value": 500},
        {"key": "svc1", "value": 400}
      ]
    },
    {
      "buckets": [
        {"key": "svc3", "value": 500},
        {"key": "svc2", "value": 300},
        {"key": "svc1", "value": 200}
      ]
    },
    {
      "buckets": [
        {"key": "svc1", "value": 100},
        {"key": "svc2", "value": 200},
        {"key": "svc3", "value": 500}
      ]
    },
    {
      "buckets": [
        {"key": "svc3", "value": 500},
        {"key": "svc2", "value": 400},
        {"key": "svc1", "value": 300}
      ]
    }
  ]
}
```

##### QUANTILE

> Функцию агрегации QUANTILE также можно применять с группировкой по полю, используя поле group_by.

**Запрос:**

```sh
grpcurl -plaintext -d '{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "field": "latency",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.5,
        0.9
      ]
    }
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Ответ:**

```json
{
  "aggs": [
    {
      "buckets": [
        {
          "quantiles": [
            300,
            500
          ],
          "value": 300
        }
      ]
    }
  ]
}
```

##### UNIQUE, COUNT

> Для `AGG_FUNC_UNIQUE`, `AGG_FUNC_COUNT` поле `field` не требуется.

**Запрос:**

```sh
grpcurl -plaintext -d '{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query": "*"
  },
  "aggs": [
    {
      "func": "AGG_FUNC_UNIQUE",
      "group_by": "service"
    },
    {
      "func": "AGG_FUNC_COUNT",
      "group_by": "service"
    }
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Ответ:**

```json
{
  "aggs": [
    {
      "buckets": [
        {"key": "svc1"},
        {"key": "svc2"},
        {"key": "svc3"}
      ]
    },
    {
      "buckets": [
        {"key": "svc1", "value": 2},
        {"key": "svc2", "value": 2},
        {"key": "svc3", "value": 1}
      ]
    }
  ]
}
```

##### COUNT (с указанием интервала)

**Запрос:**

```sh
grpcurl -plaintext -d '{
  "query": {
    "from": "2000-01-01T00:00:00Z",
    "to": "2077-01-01T00:00:00Z",
    "query":"*"
  },
  "aggs": [
    {
      "func": "AGG_FUNC_COUNT",
      "group_by": "service",
      "interval": "30s"
    }
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAggregation
```

**Ответ:**

```json
{
  "aggs": [
    {
      "buckets": [
        {
          "key": "svc1",
          "value": 2,
          "ts": "2025-08-17T11:46:00Z"
        },
        {
          "key": "svc2",
          "value": 2,
          "ts": "2025-08-17T11:46:30Z"
        },
        {
          "key": "svc3",
          "value": 2,
          "ts": "2025-08-17T11:47:00Z"
        }
      ]
    }
  ]
}
```

### `/GetHistogram`

Метод построения гистограмм.

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z"
  },
  "hist": {
    "interval": "1ms"
  }
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetHistogram
```

Пример успешного ответа:

```json
{
  "hist": {
    "buckets": [
      {
        "docCount": "1",
        "ts": "2024-12-23T18:00:36.357Z"
      },
      {
        "docCount": "9",
        "ts": "2024-12-23T18:23:41.349Z"
      }
    ]
  },
  "error": {
    "code": "ERROR_CODE_NO"
  }
}

```

### `/ComplexSearch`

Запрос, позволяющий получить сразу [документы](#search), [агрегации](#getaggregation)
и [гистограммы](#gethistogram)

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z",
    "query": "k8s_pod:seq-proxy"
  },
  "with_total": true,
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.2,
        0.8,
        0.95
      ]
    }
  ],
  "hist": {
    "interval": "1ms"
  },
  "order": 0
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/ComplexSearch
```

Пример успешного ответа:

```json
{
  "total": "5",
  "aggs": [
    {
      "buckets": [
        {
          "docCount": "6",
          "key": "seq-proxy",
          "value": 6,
          "quantiles": [
            6,
            8,
            9
          ]
        }
      ]
    }
  ],
  "hist": {
    "buckets": [
      {
        "docCount": "1",
        "ts": "2024-12-23T18:00:36.357Z"
      },
      {
        "docCount": "4",
        "ts": "2024-12-23T18:23:41.349Z"
      }
    ]
  },
  "error": {
    "code": "ERROR_CODE_NO"
  }
}
```

### `/Fetch`

Возвращает поток документов по переданному в запросе списку seq-id

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "ids": [
    "25b5c2f493010000-59024b2ba3fb9630",
    "0593adf493010000-5902ee007dfb6547"
  ]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/Fetch
```

Пример успешного ответа:

```json lines
{
  "id": "25b5c2f493010000-59024b2ba3fb9630",
  "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMCJ9",
  "time": "2024-12-23T18:23:41.349Z"
}
{
  "id": "0593adf493010000-5902ee007dfb6547",
  "data": "eyJrOHNfcG9kIjoic2VxLXByb3h5IiwgInJlcXVlc3RfdGltZSI6ICI1IiwgInRpbWUiOiAiMjAyNC0xMi0yM1QxODowMDozNi4zNTdaIn0=",
  "time": "2024-12-23T18:00:36.357Z"
}
```

#### `/Mapping`

Возвращает используемый в seq-db маппинг.

Пример запроса:

```bash
grpcurl -plaintext localhost:9004 seqproxyapi.v1.SeqProxyApi/Mapping
```

Пример успешного ответа:

```json
{
  "data": "eyJrOHNfY29udGFpbmVyIjoia2V5d29yZCIsIms4c19uYW1lc3BhY2UiOiJrZXl3b3JkIiwiazhzX3BvZCI6ImtleXdvcmQiLCJtZXNzYWdlIjoidGV4dCIsIm1lc3NhZ2Uua2V5d29yZCI6ImtleXdvcmQiLCJyZXF1ZXN0IjoidGV4dCIsInJlcXVlc3RfdGltZSI6ImtleXdvcmQiLCJyZXF1ZXN0X3VyaSI6InBhdGgiLCJzb21lb2JqIjoib2JqZWN0Iiwic29tZW9iai5uZXN0ZWQiOiJrZXl3b3JkIiwic29tZW9iai5uZXN0ZWR0ZXh0IjoidGV4dCJ9"
}
```

после декодирования base64:

```json
{
  "k8s_container": "keyword",
  "k8s_namespace": "keyword",
  "k8s_pod": "keyword",
  "message": "text",
  "message.keyword": "keyword",
  "request": "text",
  "request_time": "keyword",
  "request_uri": "path",
  "someobj": "object",
  "someobj.nested": "keyword",
  "someobj.nestedtext": "text"
}
```

#### `/Status`

Возвращает подробную информацию о всех seq-db store, используемых seq-db proxy

Пример запроса:

```bash
grpcurl -plaintext localhost:9004 seqproxyapi.v1.SeqProxyApi/Status
```

Пример успешного ответа:

```json
{
  "numberOfStores": 1,
  "oldestStorageTime": "2024-12-23T18:23:37.622Z",
  "stores": [
    {
      "host": "localhost:9234",
      "values": {
        "oldestTime": "2024-12-23T18:23:37.622Z"
      }
    }
  ]
}
```

#### `/Export`

То же самое, что и [`/Search`](#search), но потоковый

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "query": {
    "from": "2020-01-01T00:00:00Z",
    "to": "2030-01-01T00:00:00Z",
    "query": "k8s_pod:seq-db"
  },
  "size": 2,
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/Export
```

Пример успешного ответа:

```json lines
{
  "doc": {
    "id": "25b5c2f493010000-5902919c44e568be",
    "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMyJ9",
    "time": "2024-12-23T18:23:41.349Z"
  }
}
{
  "doc": {
    "id": "25b5c2f493010000-5902d3ff804c179d",
    "data": "eyJrOHNfcG9kIjoic2VxLWRiIiwgInJlcXVlc3RfdGltZSI6ICIxMiJ9",
    "time": "2024-12-23T18:23:41.349Z"
  }
}
```

## Async search gRPC API

### `/StartAsyncSearch`

Начать асинхронный поиск.

Возвращает идентификатор, с помощью которого можно получить результат выполнения поиска, отменить и удалить поиск.

Запрос похож на [`/ComplexSearch`](#complexsearch), но есть дополнительные поля:
`retention` - определяет, как долго будут доступны данные поиска.
И `with_docs` - определяет, будут ли найдены и сохранены идентификаторы документов, удовлетворяющих поисковому запросу.
`with_total` будет автоматически установлен, если в запросе указан `with_docs: true`

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "retention": "3600s",
  "query": {
    "from": "2025-07-01T05:20:00Z",
    "to": "2025-09-01T05:21:00Z",
    "query": "message:error | fields level, message"
  },
  "hist": {
    "interval": "1ms"
  },
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.2,
        0.8,
        0.95
      ]
    }
  ],
  "with_docs": true
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/StartAsyncSearch
```

Пример успешного ответа:

```json
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10"
}
```

### `/FetchAsyncSearchResult`

Получить результат выполнения асинхронного поиска.

Возвращает результат выполнения, соответствующий поисковому запросу [`/StartAsyncSearch`](#startasyncsearch).
Может быть возвращен результат частичного выполнения, если поиск еще не завершен. Поле `status` показывает текущий статус поиска.
Возможные статусы: `AsyncSearchStatusInProgress`, `AsyncSearchStatusDone`, `AsyncSearchStatusCanceled`, `AsyncSearchStatusError`.

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10",
  "size": 2,
  "offset": 0,
  "order": 0
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/FetchAsyncSearchResult
```

Пример успешного ответа:

```json
{
 "status": "AsyncSearchStatusDone",
 "request": {
  "retention": "3600s",
  "query": {
   "query": "message:peggy | fields level, message",
   "from": "2025-08-01T05:20:00Z",
   "to": "2025-09-01T05:21:00Z"
  },
  "hist": {
    "interval": "1ms"
  },
  "aggs": [
    {
      "group_by": "k8s_pod",
      "field": "request_time",
      "func": "AGG_FUNC_QUANTILE",
      "quantiles": [
        0.2,
        0.8,
        0.95
      ]
    }
  ],
  "with_docs": true
 },
 "response": {
  "total": 12,
  "docs": [
   {
    "id": "c09e878998010000-9102a3cb83f156f2",
    "data": {
     "message": "some error",
     "level": 3
    },
    "time": "2025-08-08T11:53:43.36Z"
   },
   {
    "id": "c09e878998010000-91026cf9cf3386e3",
    "data": {
     "message": "error 2",
     "level": 3
    },
    "time": "2025-08-08T11:53:43.36Z"
   }
  ],
  "aggs": [
    {
      "buckets": [
        {
          "docCount": "6",
          "key": "seq-proxy",
          "value": 6,
          "quantiles": [
            6,
            8,
            9
          ]
        }
      ]
    }
  ],
  "hist": {
    "buckets": [
      {
        "docCount": "1",
        "ts": "2024-12-23T18:00:36.357Z"
      },
      {
        "docCount": "4",
        "ts": "2024-12-23T18:23:41.349Z"
      }
    ]
  }
 },
 "progress": 1,
 "disk_usage": "537",
 "started_at": "2025-08-08T11:53:52.542336Z",
 "expires_at": "2025-08-08T12:53:52.542336Z"
}
```

> Поля `response.docs` и `response.total` будут сохранены и возвращены только если в запросе [`/StartAsyncSearch`](#startasyncsearch) было указано `with_docs: true`

### `/GetAsyncSearchesList`

Получить список асинхронных поисков.

Возвращает список асинхронных поисков, отфильтрованный в соответствии с полями запроса.
Пустой запрос вернет все доступные поиски.

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "status": null,
  "size": 2,
  "offset": 0,
  "ids": ["c28c97d1-117a-45dc-a0cf-d080f22b2a10", "8e68d3d7-8e85-44a9-9b09-8de248a3b414"]
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/GetAsyncSearchesList
```

Пример успешного ответа:

```json
{
 "searches": [
  {
   "searchId": "c28c97d1-117a-45dc-a0cf-d080f22b2a10",
   "status": "AsyncSearchStatusInProgress",
   "request": {
    "retention": "3600s",
    "query": {
     "query": "message:error | fields level, message",
     "from": "2025-07-01T05:20:00Z",
     "to": "2025-08-01T05:21:00Z"
    },
    "aggs": [],
    "with_docs": true
   },
   "startedAt": "2025-08-08T11:49:20.707200Z",
   "expiresAt": "2025-08-08T12:49:20.707200Z",
   "progress": 0.89,
   "diskUsage": "282"
  },
  {
   "searchId": "8e68d3d7-8e85-44a9-9b09-8de248a3b414",
   "status": "AsyncSearchStatusDone",
   "request": {
    "retention": "3600s",
    "query": {
     "query": "message:error",
     "from": "2025-07-01T05:20:00Z",
     "to": "2025-08-01T05:21:00Z"
    },
    "aggs": [],
    "with_docs": false
   },
   "startedAt": "2025-08-08T11:48:49.488244Z",
   "expiresAt": "2025-08-08T12:48:49.488244Z",
   "progress": 1,
   "diskUsage": "283"
  }
 ]
}
```

### `/CancelAsyncSearch`

Отменить асинхронный поиск.

Отменяет асинхронный поиск. Выполнение отмененного поиска будет остановлено. Нельзя отменить завершенный поиск.

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10"
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/CancelAsyncSearch
```

Пример успешного ответа:

```json
{}
```

В случае успеха будет возвращен пустой ответ.

### `/DeleteAsyncSearch`

Удалить асинхронный поиск.

Помечает поиск истекшим. Данные поиска будут удалены при следующей итерации обслуживания.

Пример запроса:

```bash
grpcurl -plaintext -d '
{
  "search_id": "c28c97d1-117a-45dc-a0cf-d080f22b2a10"
}' localhost:9004 seqproxyapi.v1.SeqProxyApi/DeleteAsyncSearch
```

Пример успешного ответа:

```json
{}
```

В случае успеха будет возвращен пустой ответ.
