---
id: seq-ql
---

# seq-ql

## Full-text Search

Full-text search in seq-ql allows filtering results based on document tokens.
Queries can include exact phrases or keywords separated by spaces.
The behavior depends on the index type; more details on token formation can be found in
the [index types](03-index-types.md) documentation.
When performing a full-text search, the system automatically selects results that match the specified text.

Search queries are case-insensitive by default.
To change this behavior, use the `--case-sensitive` flag, but it affects only new documents.

### String Literals

In seq-ql, different syntaxes can be used for string literals when specifying values in filters.

1. **Without quotes** — can be used if the field or value consists of a single word without spaces or special
   characters:
   ```seq-ql
   key: value
   ```

2. **Single quotes (`'`)** — allow specifying fields or values containing spaces and special characters:
   ```seq-ql
   key: 'value with spaces'
   ```
   Inside quotes, escape sequences can be used, which must always start with the ` \ ` character. Example:
   ```seq-ql
   # Equivalent to search: key:значение
   key: '\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435'
   ```
   Supported escape sequences:
    * [Wildcard](#wildcards) characters – `\*`
    * Quotes and slashes – ` \" `, ` \' `, ` \\ `
    * Unicode – `\u` for 4-byte characters, `\U` for 8-byte characters
    * Control characters – `\n`, `\r`
    * Byte – `\x`

3. **Double quotes (`"`)** — similar to single quotes but allow single quotes inside the string:
   ```seq-ql
   "key": "value with 'quotes'"
   ```

4. **Backticks (`` ` ``)** — used for fields and values containing both single and double quotes:
   ```seq-ql
   `key with space`:`value with "double" and 'single' quotes`
   ```
   Important: backticks do not escape [wildcard](#wildcards) and escape sequences, so the following examples
   are equivalent:
   ```seq-ql
   key: `\n` or key: `*`
   ```
   ```seq-ql
   key: '\\n' or key: '\*'
   ```

## Logical Operators

seq-ql supports logical operators for more precise search filtering.
The operators `and`, `or`, and `not` allow combining and refining queries:

- `and` — requires both conditions to be true.
- `or` — returns results if at least one condition is met.
- `not` — excludes results that match a specific condition.

Examples:

```seq-ql
message:error and level:critical
message:login or message:logout
not level:debug
```

## Wildcards

Wildcards in seq-ql allow for partial matching in search.
The language supports the following symbols:

- `*` — replaces any number of characters.

These symbols can be used to search within tokens or parts of tokens.
For example, a query on the [keyword](03-index-types.md#keyword) index `source_type:access*` will match all documents starting
with `access`.

## Filter `range`

seq-ql enables range filtering to limit data by value, which is particularly useful for numeric fields such as sizes or
timestamps.
Ranges are specified using square or round brackets:

- `[a, b]` — includes both ends of the range.
- `(a, b)` — excludes both ends of the range.

Example of range usage:

```seq-ql
bytes:(100, 1000]
bytes:[100, 1000)
```

## Filter `in`

seq-ql allows using the `in` filter to filter a list of tokens.
The syntax for full-text search is the same as in [full-text search](#full-text-search) and supports all types of string
literals.
Search tokens must be separated by commas.

Example usage of `in`:

```seq-ql
level:error and k8s_namespace:in(default, kube-system) and k8s_pod:in(kube-proxy-*, kube-apiserver-*, kube-scheduler-*)

trace_id:in(123e4567-e89b-12d3-a456-426655440000, '123e4567-e89b-12d3-a456-426655440001', "123e4567-e89b-12d3-a456-426655440002",`123e4567-e89b-12d3-a456-426655440003`)
```

## Pipes

Pipes in seq-ql are used to sequentially process data.
Pipes enable data transformation, enrichment, filtering, aggregation, and formatting of results.

#### `fields` pipe

The `fields` pipe allows removing unnecessary fields from documents in the output.
This is useful when only specific fields are needed, especially when exporting large datasets to reduce network load.

The listed fields may not be present in the [mapping](03-index-types.md), and their absence does not affect the performance of
the `fields` pipe.

Example:

```seq-ql
source_type:access* | fields user, KB
```

In the final result, each document will contain only the `user` and `KB` fields.

To remove only specific fields, use the `except` keyword:

```seq-ql
source_type:access* | fields except payload, cookies
```

In this example, the `payload` and `cookies` fields will be excluded from the result.

## Comments

Comments are user-provided text that will be ignored when executing a query.  
Comments must start with the `#` keyword, followed by the comment text, which must end with a newline character.

Example:

```seq-ql
# Exporting debug logs for file.d
service: file-d and message: 'Event sample' and level: error # Filtering file.d logs by "error" level
| fields event # Returning the 'event' field to retrieve all debug logs for file.d
```

## Query Optimization Recommendations for seq-ql

Optimizing seq-ql queries enables efficient data processing and faster access to results, reducing system load.

### Reducing Returned Fields

Specify only the fields you need to reduce data volume and accelerate query processing.
Use the `fields` formatting pipe to select only important fields:

```seq-ql
source_type:access* | fields KB, status, timestamp
```

In this example, the final result includes only `KB`, `status`, and `timestamp`, reducing output time.
