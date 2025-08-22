---
id: aggregations
---

# Aggregations

seq-db support various types of aggregations: functional aggregations, histograms and timeseries. Each of the types
relies on the usage of the inverted-index, therefore to calculate aggregations for the fields, the field must be
indexed. However, because of that, seq-db can very quickly retrieve and aggregate data.

## Function aggregations

Aggregations allow the computation of statistical values over document fields that match the query. E.g. calculating
number of logs written by each service in the given interval, or all unique values of the field.

seq-db supports various aggregation functions:

- `AGG_FUNC_SUM` — sum of field values
- `AGG_FUNC_AVG` — average value of the field
- `AGG_FUNC_MIN` — minimum value of the field
- `AGG_FUNC_MAX` — maximum value of the field
- `AGG_FUNC_QUANTILE` — quantile value for the field
- `AGG_FUNC_UNIQUE` — computation of unique field values (not supported in timeseries)
- `AGG_FUNC_COUNT` — number of documents for each value of the field

For the API of the functions, please refer to [public API](10-public-api.md#aggregation-examples)

To better understand how aggregations work, let's illustrate examples with identical SQL queries.

### Sum, average, minimum, maximum, quantile

Calculation of the aforementioned aggregations requires:

- `AGG_FUNC` which is one of `AGG_FUNC_SUM`, `AGG_FUNC_AVG`, `AGG_FUNC_MIN`, `AGG_FUNC_MAX`, `AGG_FUNC_QUANTILE`,
- `aggregate_by_field` - the field on which aggregation will be applied
- `group_by_field` - the field by which values will be grouped
- `filtering_query`- query to filter only relevant logs for the aggregation
- `quantile` - only for the `AGG_FUNC_QUANTILE`

In general, this translates to the following sql query:

```sql
SELECT <group_by_field>, AGG_FUNC(<aggregate_by_field>),
FROM db
GROUP BY <group_by_field>
WHERE <filtering_query>
```

Considering real-world example, we may want to calculate average response time for services having `response_time`
field, then we will write the following query:

```sql
SELECT service, AVG(response_time)
FROM db
GROUP BY service WHERE response_time:* -- meaning that `response_time` field exists in logs
```

### Count, unique

Count and unique aggregations are very similar to the above examples, except for those aggregation there is no need to
have an
additional `group_by_field`, since we are already grouping by `aggregate_by_field`.

Identical sql query for the `AGG_FUNC_COUNT` aggregation:

```sql
SELECT <aggregate_by_field>, COUNT (*)
FROM db
GROUP BY <aggregate_by_field>
WHERE <filtering_query>
```

Considering real-world example, we may want to calculate number of logs for each logging level (`debug`, `info`, etc.)
for
the particular service, e.g. `seq-db`, then we can write the following query:

```sql
SELECT level, COUNT(*)
FROM db
GROUP BY level WHERE service:seq-db
```

## Histograms

Histograms allow users to visually understand amount of logs in each sub-interval. E.g. visualize number of logs
particular service for the given interval of time

For the API of the functions, please refer to [public API](10-public-api.md#gethistogram)
