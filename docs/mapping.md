## Mapping

## Наш маппинг

Маппинг с явным указанием типа полей: ```text```, ```keyword```, ```path```.

### Пример

Маппинг:

```yaml
mapping-list:
    - name: "message"
      types:
      - type: "text"
      - title: "keyword"
        type: "keyword"
        size: 18
    - name: level
      type: keyword
    - name: foo
      type: keyword
    - name: uri
      type: path
```

Есть возможность указать несколько типов для одного поля. В этом случае тип с пустым `title` будет считаться основным.
Для типов с заполненным `title` будут созданы новые поля, названия которых будут состоять из значений полей `name` и `title`, соединенных через `.`.
В случае из примера - `message.keyword`

Запишем такой документ (он получит ```id = N```):

```json
{
    "message": "hello world",
    "some_number": 1,
    "level": "info",
    "foo": "aaa bbb ccc",
    "uri": "/my/path"
}
```

Это приведет к созданию следующей метаинформации:

```json
{
    id: N,
    tokens: [
        "message:hello",
        "message:world",
        "message.keyword:hello world",
        "level:info",
        "foo:aaa bbb ccc"
        "uri:/my",
        "uri:/my/path",
    ]
}
```

Заметим, что:

- появились токены для поля `message.keyword`, которое не было указано в исходном документе, но было дочерним для поля `message` в маппинге.
- данные в поле ```foo``` индексируются как единое целое (т.к. в маппинге ```foo: keyword```), хотя могут
  быть разбиты на несколько слов
- для поля `uri` есть по токену на каждую секцию пути
- поле ```some_number``` вообще не индексируется
