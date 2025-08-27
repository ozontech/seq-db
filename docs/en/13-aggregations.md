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
SELECT <group_by_field>, AGG_FUNC(<aggregate_by_field>)
FROM db
WHERE <filtering_query>
GROUP BY <group_by_field>
```

Considering real-world example, we may want to calculate average response time for services having `response_time`
field, then we will write the following query:

```sql
SELECT service, AVG(response_time)
FROM db
WHERE response_time:* -- meaning that `response_time` field exists in logs
GROUP BY service
```

### Count, unique

Count and unique aggregations are very similar to the above examples, except for those aggregation there is no need to
have an
additional `group_by_field`, since we are already grouping by `aggregate_by_field`.

Identical sql query for the `AGG_FUNC_COUNT` aggregation:

```sql
SELECT <aggregate_by_field>, COUNT (*)
FROM db
WHERE <filtering_query>
GROUP BY <aggregate_by_field>
```

Considering real-world example, we may want to calculate number of logs for each logging level (`debug`, `info`, etc.)
for
the particular service, e.g. `seq-db`, then we can write the following query:

```sql
SELECT level, COUNT(*)
FROM db
WHERE service:seq-db
GROUP BY level
```

## Histograms

Histograms allow users to visually interpret the distribution of logs satisfying given query. E.g. number of logs of the
particular service for the given interval of time.

Histograms can be queried separately, using [GetHistogram](10-public-api.md#gethistogram) or with documents and
functional aggregations using [ComplexSearch](10-public-api.md#complexsearch) gRPC handlers.

For the detailed API and examples, please refer to [public API](10-public-api.md#gethistogram)

## Timeseries

Timeseries allow to calculate aggregations for intervals and visualize them. They are something in between histograms
and functional aggregations: they allow to simultaneously calculate multiple histograms for the given aggregate
functions.

Consider the previous example of histograms, where we visualized number of logs over time only for one service at a
time. Using the power of timeseries, we can calculate number of logs for each service simultaneously, using the
`AGG_FUNC_COUNT` over `service` field.

Another example of using timeseries is visualizing number of logs for each log-level over time. It may be exceptionally
useful, when there is a need to debug real-time problems. We can simply visualize number of logs for each level and find
unusual spikes and logs associated with them.

Because timeseries are basically aggregations, they have the same API as aggregations, except a new `interval` field is
present to calculate number of buckets to calculate aggregation on. For the details, please refer
to [public API](10-public-api.md#aggregation-examples)
