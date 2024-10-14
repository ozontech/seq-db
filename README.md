# Sequence DB

База данных для хранения документов. API для чтения (msearch) и записи (bulk) до некоторой степени
совместим с Elasticsearch API. Документация в папке docs.

## Changelog

За историей изменений можно следить по тэгам <https://github.com/ozontech/seq-db/-/tags>

## Способы сборки

- `make build-image`: Сборка Docker image, выполняется с переменными `GOOS=linux GOARCH=amd64`.
- `make push-image`: Сборка Docker image и публикация в gitlab registry.

## Запустить локально

### Через конфигурацию в IDE

```bash
# store
go run ./cmd/seq-db --mode=store --mapping=./tests/data/mappings/logging-new.yaml --addr=127.0.0.1:9234 --debug-addr=":54231"
# proxy
go run ./cmd/seq-db --mode=ingestor --addr=localhost:9123 --mapping=tests/data/mappings/logging-new.yaml --hot-stores=localhost:9234 --proxy-grpc-addr=localhost:9542
```

### Через Docker

```bash
make build-binaries
docker-compose up --build store proxy # --build чтобы изменения в бинарнике заапдейтились в контейнере
# или
docker-compose up --build -V store proxy # -V чтобы очистить все файлы данных
```

Запустит два контейнера: `store` и `proxy`. Данные будут храниться в папке `./data`. В эту папку можно подложить
готовые данные, они подхватятся store (или не подхватятся - тогда можно подебажить). Смотри раздел копирование из
окружения.

Proxy будет слушать запросы на `http://localhost:9002`.

## Примеры

- запрос для сохранения данных [`store.sh`](tests/data/store.sh)
- для поиска [`search.sh`](tests/data/search.sh)
- подробное [описание](query/examples.md) с примерами

## Запустить тесты

Для быстрых тестов используй `make ci-tests`. Для длинных тестов и бенчей, следует запускать их индивидуально.

## Трейсинг

Для того, чтобы работал трейсинг, необходимо указать в env: `JAEGER_AGENT_HOST`, `JAEGER_AGENT_PORT`.

Сервис будет называться `seq-db-store`.

### Копирование из окружения

Может возникнуть необходимость скопировать данные с какого-либо окружения (`prod` или `dev`). Это можно сделать
двумя способами:

Через kubectl (пример для окружения `dev`)

```bash
kubectl exec seq-db-z501-0 -- /bin/bash -c 'cd /data; tar -zc seq-db-01G8TAYNVV47C5F52N9RVMHBQN.*; sleep 5' | tar -x -C ./
```

Без sleep эта команда может сфейлиться (kubectl отдаёт не весь вывод и tar падает с unexpected eof).
Если всё равно падает, можно увеличить sleep и попытаться ещё раз.

Альтернативный вариант:

```bash
kubectl -n logging get statefulset seq-db-z26 -o yaml | grep claim
kubectl -n logging get pvc | grep seq-db-z26-claim
kubectl -n logging get pv pvc-8c42851c-e741-4b4c-a997-8b4c9a9b44c3 -o yaml
```

Посмотреть на какой ноде лежит volume и какой него путь. Теперь можно скопировать оттуда данные через rsync.
