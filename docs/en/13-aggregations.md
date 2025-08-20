---
id: aggregations
---

# Aggregations

## Overview

Aggregations allow the computation of statistical values over document fields that match the query. seq-db supports various aggregation functions:

- `AGG_FUNC_SUM` — sum of field values
- `AGG_FUNC_AVG` — average value of the field
- `AGG_FUNC_MIN` — minimum value of the field
- `AGG_FUNC_MAX` — maximum value of the field
- `AGG_FUNC_QUANTILE` — quantile computation for the field
- `AGG_FUNC_UNIQUE` — computation of unique field values (not supported in timeseries)
- `AGG_FUNC_COUNT` — count number of documents per group

For the API of the functions, please refer to [public API](10-public-api.md#aggregation-examples)