---
id: quickstart
slug: /
---

# Быстрый запуск

Добро пожаловать в руководство по быстрому запуску seq-db! Всего через несколько минут вы узнаете, как:

- Быстро запустить seq-db
- Записывать логи
- Выполнять поисковые запросы

## Запуск seq-db

### Запуск в единственном экземпляре

seq-db можно быстро запустить в docker-контейнере:

Перед запуском нужно удостовериться, что создан файл конфигурации:

config.yaml:

```yaml
storage:
  data_dir: /seq-db-data

mapping:
  path: auto
```

Следующая команда скачает образ seq-db из GitHub Container Registry (GHCR) и запустит контейнер:

```bash
docker run --rm \
  -p 9002:9002 \
  -p 9004:9004 \
  -p 9200:9200 \
  -v "$(pwd)"/config.yaml:/seq-db/config.yaml \
  -it ghcr.io/ozontech/seq-db:latest --mode single --config=config.yaml
```

Обратите внимание, что флаг `--mode single` запускает seq-db в единственном исполняемом файле, а не в кластерном режиме.
И настройка `mapping.path: auto` позволяет автоматически индексировать все поля документа как `keyword` тип.

Чтобы узнать больше о поддерживаемых видах индексов, смотрите [документацию](./03-index-types.md).

### Кластерный режим

Seq-db можно запустить в кластерном режиме, используя два основных компонента:

- seq-db proxy - принимает запросы и маршрутизирует их согласно конфигурации кластера
- seq-db store - отвечает за хранение и получение данных

Используйте флаг `--mode`, чтобы выбрать определенный способ запуска: `proxy` или `store`.

Перед запуском нужно создать файл конфигурации:

config.yaml:

```yaml
cluster:
  hot_stores:
    - seq-db-store:9004

mapping:
  path: auto
```

Далее нужно запустить docker compose пример:

```yaml
services:
  
  seq-db-proxy:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    ports:
      - "9002:9002" # Default HTTP port
      - "9004:9004" # Default gRPC port
      - "9200:9200" # Default debug port
    command: --mode proxy --config=config.yaml
    depends_on:
      - seq-db-store
  
  seq-db-store:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    command: --mode store --config=config.yaml
```

Также можно запустить файл `docker-compose.yaml` из директории `quickstart`, который содержит примерно такую же конфигурацию.

Смотрите [настройки кластера](02-configuration.md#cluster-configuration) и [долгосрочное хранилище](07-long-term-store.md),
чтобы узнать больше о возможных способах конфигурации seq-db.

Обратите внимание, что мы установили настройку `mapping.path: auto`, это сделано для упрощения запуска, эта настройка не предназначена для использования в продакшене.
Поэтому мы рекомендуем прочитать больше о [маппингах и индексации полей](03-index-types.md) и [архитектуре](13-architecture.md)

## Запись документов в seq-db

### Запись документов при помощи `curl`

seq-db поддерживает elasticsearch `bulk` API, поэтому вставить документы в seq-db, запущенную на порту 9002, можно следующим образом:

```bash
curl --request POST \
  --url http://localhost:9002/_bulk \
  --header 'Content-Type: application/json' \
  --data '{"index" : {"unused-key":""}}
{"k8s_pod": "app-backend-123", "k8s_namespace": "production", "k8s_container": "app-backend", "request": "POST", "request_uri": "/api/v1/orders", "message": "New order created successfully"}
{"index" : {"unused-key":""}}
{"k8s_pod": "app-frontend-456", "k8s_namespace": "production", "k8s_container": "app-frontend", "request": "GET", "request_uri": "/api/v1/products", "message": "Product list retrieved"}
{"index" : {"unused-key":""}}
{"k8s_pod": "payment-service-789", "k8s_namespace": "production", "k8s_container": "payment-service", "request": "POST", "request_uri": "/api/v1/payments", "message": "failed"}
'
```

## Поиск документов

И в конце выполним простой поисковый запрос, который фильтрует записанные логи по полю `message`.

```bash
curl --request POST   \
  --url http://localhost:9002/search \
  --header 'Content-Type: application/json' \
  --header 'Grpc-Metadata-use-seq-ql: true' \
  --data-binary '
  {
    "query":{
      "query":"message:failed",
      "from": "2025-02-11T10:30:00Z",
      "to": "2030-11-25T17:50:30Z"
    },
    "size": 100,
    "offset": 0
  }'
```

## Запуск seq-db вместе с seq-ui

seq-ui это бэкенд для пользовательского интерфейса.

Чтобы запустить seq-db в связке с seq-ui, нужно создать файл конфигурации для seq-ui, config.seq-ui.yaml:

```yaml
server:
  http_addr: "0.0.0.0:5555"
  grpc_addr: "0.0.0.0:5556"
  debug_addr: "0.0.0.0:5557"
clients:
  seq_db_addrs:
    - "seq-db-proxy:9004"
  seq_db_timeout: 15s
  seq_db_avg_doc_size: 100
  request_retries: 3
  proxy_client_mode: "grpc"
  grpc_keepalive_params:
    time: 10s
    timeout: 10s
    permit_without_stream: true
handlers:
  seq_api:
    seq_cli_max_search_limit: 10000
    max_search_limit: 1000
    max_search_total_limit: 100000
    max_search_offset_limit: 100000
    max_export_limit: 10000
    max_parallel_export_requests: 1
    max_aggregations_per_request: 3
```

Также нужен файл конфигурации для seq-db, config.yaml:

```yaml
cluster:
  hot_stores:
    - seq-db-store:9004

mapping:
  path: auto
```

Далее нужно запустить этот docker compose пример:

```yaml
services:

  seq-ui:
    image: ghcr.io/ozontech/seq-ui:latest
    volumes:
      - ${PWD}/config.seq-ui.yaml:/seq-ui/config.yaml
    ports:
      - "5555:5555" # Default HTTP port
      - "5556:5556" # Default gRPC port
      - "5557:5557" # Default debug port
    command: --config config.yaml
  
  seq-db-proxy:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    ports:
      - "9002:9002" # Default HTTP port
      - "9004:9004" # Default gRPC port
      - "9200:9200" # Default debug port
    command: --mode proxy --config=config.yaml
    depends_on:
      - seq-db-store
  
  seq-db-store:
    image: ghcr.io/ozontech/seq-db:latest
    volumes:
      - ${PWD}/config.yaml:/seq-db/config.yaml
    command: --mode store --config=config.yaml
```

Чтобы узнать больше о seq-ui, смотрите соответствующую документацию.

## Что дальше

seq-db имеет множество других полезных функций для работы с логами. Вот несколько примеров:

- Собственный язык запросов - [seq-ql](05-seq-ql.md) - поддерживает конвейеры (pipes), запросы диапазонов, подстановочные символы (*) и другое.
- Поддержка построения различных агрегаций: сумма, среднее, квантили и так далее - [aggregations](13-aggregations.md)
- Возможность объединить в один запрос получение документов, агрегаций и гистограмм - [complex-search](10-public-api.md#complexsearch)
- Получение документов по ID - [fetch](10-public-api.md#fetch)
