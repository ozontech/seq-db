# Index types and mappings
seq-db doesn't index any fields from the ingested data by default.
Instead, indexing is controlled through a special file called the *mapping file*.
The mapping file specifies the indexed fields and the used index types.

## Index types
Below is a description of mapping types seq-db currently supports. There are several index types with different behaviors.

### `keyword` mapping type
The `keyword` mapping type treats the whole value as a single token, without breaking it up.
Usually used for content like statuses, namespaces, tags or any other data where a full match search is required.
Note that `keyword` index should be used with care with high-cardinality values like `trace_id`, `span_id` or
other unique request identifiers, as the indexing of these fields might blow up the index.

Example of a mapping for a keyword field:

```yaml
mapping-list:
  - name: level
    type: keyword
```

### `path` mapping type
This mapping type indexes hierarchical path-like values.
It is very similar to the
elasticsearch's [path tokenizer](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pathhierarchy-tokenizer.html).
When a field is indexed with the `path` index, its value is broken
into hierarchical terms.

Used for searching by beginning of a path or full path. For example, the following documents will match query `uri:"/my/path"`:
```json
[
  {"uri": "/my/path"},
  {"uri": "/my/path/1"},
  {"uri": "/my/path/1/"},
  {"uri": "/my/path/1/one"}
]
```

Example of a mapping for a path field:

```yaml
mapping-list:
  - name: uri
    type: path
```

### `text` mapping type

Index for content like error messages or request bodies where full-text search is required.
This mapping type is used to index fields containing unstructured, natural language text, as well as human-written messages, descriptions,
error messages and other free-text fields.

For example, the search query `message:"error"` will emit all the documents that contain token `error`. And query `message:"error code"` will emit all the documents that contain both `error` and `code`.

Example of a mapping for a text field:

```yaml
mapping-list:
  - name: message
    type: text
```


### `exists` mapping type

Used when the presence of the field is important and not the value.

This mapping type should be used when a field might or might not be
present in a message.
For example, for query `_exists_:service` all documents that have a `service` field will be found regardless of the field value.

Note that the `_exists_` query will work on other types of mapping as well.

Example of a mapping for an exists field:

```yaml
mapping-list:
  - name: service
    type: exists
```

## Configuration parameters

* `--partial-indexing` - if true, will index only the first part of long fields, otherwise will skip entry if length of field value is greater than threshold.

* `--max-token-size` - max size of a single token, default is 72.

* `--case-sensitive` - if false, will convert values to lower case.

* constant `consts.MaxTextFieldValueLength` - limits maximum length of the text field value. Current threshold is 32768 bytes.

## Object indexing
seq-db can also index logs containing nested structured data.
In this case, the parent field should have the `object` index type,
and contain a `mapping-list` object inside, that would specify
how exactly its nested fields should be indexed.

For example:

```yaml
mapping-list:
  - name: "myobject" # key that contains nested json data
    type: "object"
    mapping-list: # mapping for the nested fields
      - type: "keyword"
        name: "nested"
      - type: "text"
        name: "nestedtext"
```

### Field names containing dots and nested objects

seq-db doesn't differentiate between fields indexed
in a nested object and fields containing a dot in their name.
For example, the mapping displayed below, will index both `name`
keys nested inside the object with the key `user`, and the `user.name` field
inside the root log.

```yaml
mapping-list:
  - name: user
    type: object
    mapping-list:
      - type: keyword
        name: name
  - user.name: keyword
```


## Multiple indexes on a single field

A single field can be indexed with multiple types at the same time.
This allows to combine multiple indexing strategies enabling more flexible
search capabilities.

For instance, a field can be indexed as both `keyword` and `text`,
allowing both full-text search and exact filtering.

If multiple indexing types are used, the additional indexing
type should have titles, that will be used during data search.
Say we have this mapping:

```yaml
mapping-list:
    - name: message
      types:
      - type: text
      - title: keyword
        type: keyword
        size: 18
```

In this case, the type with empty `title` will be default one.

For types with `title` new "implicit" fields will be created: `message.keyword` in our case. Use `message.keyword` to search over message as keyword and `message` to search as text.

The title of implicit field consists of values of `name` and `title` joined together with a dot between them.


## Illustrated mapping example

Let's walk through a practical example.
We'll define a mapping and analyze how seq-db would index a sample document.

Say we have this example mapping with explicit field types: `text`, `keyword`, `path`, `exists`.

```yaml
mapping-list:
    - name: message
      types:
      - type: text
      - title: keyword
        type: keyword
        size: 18
    - name: level
      type: keyword
    - name: foo
      type: exists
    - name: bar
      type: keyword
    - name: uri
      type: path
```

There is also a `size` field that allows you to specify the maximum size of the input data.

If `size` is not set, the [default](#configuration-parameters) will be used.


## Indexing internals

Let's write a document using [mapping](#illustrated-mapping-example) (ID of document will be ```id = N```):

```json
{
    "message": "hello world",
    "some_number": 1,
    "level": "info",
    "foo": "aaa bbb ccc",
    "bar": "ddd eee fff",
    "uri": "/my/path"
}
```

Following metadata will be created:

```json
{
  id: N,
  tokens: [
    "_exists_:message",
    "message:hello",
    "message:world",
    "_exists_:message.keyword",
    "message.keyword:hello world",
    "_exists_:level",
    "level:info",
    "_exists_:foo",
    "_exists_:bar",
    "bar:ddd eee fff",
    "_exists_:uri",
    "uri:/my",
    "uri:/my/path",
  ]
}
```

Note that:

* There are additional `_exists_` tokens for each field.
* There are tokens for `message.keyword` field, which wasn't present in the original document, but was in mapping as "implicit" type for `message` field.
* `foo` field has only `_exists_` token.
* Data in the `bar` field indexed as single token because it is `keyword` field.
* There are multiple tokens for `uri` field, a token for each segment of the path.
* Field `some_number` is not indexed, because it is not in the mapping.
