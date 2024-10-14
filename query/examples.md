Все запросы из формата JSON, преобразуются в структуру
следующего вида:
```
type ElasticQuery struct {
Query     []byte        - Результат преобразования JSON запроса к формату ElasticSearch c добавлением индексов
Offset    int           - Номер элемента, начиная с которого выводим результат
Size      int           - Количество отображаемых элементов
From      *time.Time    - Временные пределы, в которых должна находиться
To        *time.Time      запрашиваемая информация
WithTotal bool          - [на данный момент не поддерживается]
Explain   bool          - Флаг, используемый для дебага [логирует запрос]
AggName   string        - Имя агрегации
AggFilter string        - [на данный момент не поддерживается]
AggField  string        - Поле по которому проводится агрегация
Interval  time.Duration - Интервала для гистограммы
}
```


В зависимости от заполненности полей структуры ElasticQuery, в
дальнейшем запрос преобразуется в один из следующих типов:

1. SearchRequestReqular     - получение результатов поиска
2. SearchRequestAggregation - получение агрегации результатов поиска по фиксированным полям
3. SearchRequestHistogram   - получение гистограммы составленной из результатов поиска

(Тип один и тот же, SearchRequest, но разная заполненность)
При каких условиях ElasticQuery преобразуется в один из
этих трёх запросов?
- Нет поля AggName - преобразуется к SearchRequestReqular,
- Есть AggName, но нет Interval - к SearchRequestAggregation,
- Заполнены оба поля - к SearchRequestHistogram.

Какие поля для каждого из типов запросов можно[необходимо] заполнять(AggFilter и WithTotal не учитываем):
Под необходимо подразумевается то, что запрос будет фиксированного типа и вернется без ошибок.
- SearchRequestReqular:     Query, Offset, Size, From, To, Explain [From, To]
- SearchRequestAggregation: Query, From, To, Explain, AggName, AggField [From, To, AggName, AggField]
- SearchRequestHistogram:   Query, From, To, Explain, AggName, AggField, Interval [From, To, AggName, AggField, Interval]

Значения по умолчанию(поля, которые можно не задавать):
- Offset  - 0
- Size    - 100

Примеры запросов для каждого из типов:
- Перед началом посмотрите на содержимое [`store.sh`](../tests/data/store.sh), здесь приведены примеры запросов на сохранение данных. 
- Для лучшего понимания логики работы запросов вы можете запустить этот скрипт и поделать запросы на тренировочном наборе данных.

SearchRequestReqular

1. Запрос со всеми заполненными полями и индексом (вывести 5 элементов, начиная с элемента с номером 1(второй элемент по счету))
```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"query":{"bool":{"must":[{"query_string":{"query":"message:prefix1*"}},{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}},"from":1,"size":5}
'
```

- Поле запроса query, "message:prefix1*" - указан тип поля "message" и шаблон для поиска соответствий "prefix1*"
- Range - интервал времени в котором рассматриваем записи
- From  - начиная с какого элемента выводим информацию
- Size  - количество выводимых элементов

2. Запрос с Offset и Size по умолчанию

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"query":{"bool":{"must":[{"query_string":{"query":"message:prefix1*"}},{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}}}
'
```


3. Запрос с произвольным Query

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"query":{"bool":{"must":[{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}}}
'
```

4. Запрос начиная с search_after(еще один поддерживаемый способ фиксации интервала времени)

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"query":{"bool":{"must":[{"query_string":{"query":"message:prefix1*"}}]}},"search_after":[1624743354891000],"from":1,"size":5}
'
```

- search_after можно применять для любого типа запросов, вы задаете только левую границу по времени, вторая задается автоматически, как текущее время. 
- На вход принимается время в микросекундах, прошедшее с 1 января 1970 года по всемирному координированному времени(Unix время).

SearchRequestAggregation

1. Агрегация со всеми заполненными полями

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"aggregations":{"aggName":{"terms":{"field":"message"}}},"query":{"bool":{"must":[{"query_string":{"query":"message:prefix*"}},{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}}}
'
```

- aggName - имя агрегации
- message - тип поля по которому проводим агрегацию

2. Поиск по одному полю(message), агрегация по другому(component)

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"aggregations":{"aggName":{"terms":{"field":"component"}}},"query":{"bool":{"must":[{"query_string":{"query":"message:prefix*"}},{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}}}
'
```

3. Запрос без поля query (не рабочий вариант)

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"aggregations":{"aggName":{"terms":{"field":"component"}}},{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}}}
'
```

4. Пустое поле AggField, агрегации не происходит  
```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"aggregations":{"aggName":{"terms":{"field":""}}},"query":{"bool":{"must":[{"query_string":{"query":"message:prefix*"}},{"range":{"timestamp":{"from":"2006-01-02 15:04:05.999","to":"2026-01-02 15:04:05.999"}}}]}}}
'
```

SearchRequestHistogram

1. Гистограмма со всеми заполненными полями

```bash
curl "localhost:9002/_msearch" -H 'Content-Type: application/json' -d \
'{"index":"index-main"}
{"aggregations":{"component":{"date_histogram":{"interval":"1h"}},"terms":{"field":"message"}},"query":{"bool":{"must":[{"query_string":{"query":"component:*"}},{"range":{"timestamp":{"from":"2022-09-06 10:04:05.999","to":"2022-09-06 15:04:05.999"}}}]}}}
'
```

- interval принимает длительность(без промежутков/пробела). Примеры: "1s" - 1 секунда, "10m" - 10 минут , "10y" - 10 лет.
- Доступные промежутки времени: s - секунда, m - минута, h - час, d - день, w - неделя, M - месяц(30 дней), q - квартал(91 день), y - год(365 дней).