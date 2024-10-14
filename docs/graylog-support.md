# How SeqDB & Graylog work together

## Intro
Graylog is a very popular tool that consumes logs from different inputs, then uses ElasticSearch to store/index/search logs. At Ozon we use SeqDB as the main database to store and analyze logs instead of ElastcSearch (why we do that is beyond the scope of this document).
As of version 2.3 Graylog [uses](https://docs.graylog.org/docs/elasticsearch) the HTTP protocol to interact with the ES cluster. 
To be able to trick Graylog into believing that it is actually interacting with an ElasticSearch cluster and enable some specific features, few additional handlers were added to the Ingestor API.

## Supported Graylog Version
**Important!** As of 6 Apr, 2022, the supported features were tested **only with Graylog 4.2**.
Support for newer graylog versions should be tested additionally.

## Setup & Run 
Here we assume that Graylog 4.2 is launched using docker. A working `docker-compose.yaml` example is provided in the root of this repo. 
It's important to mention a couple of things that should be done correctly before starting the Graylog Server.
1) These two environment variables shouldn't be changed because SeqDB only works correctly when Graylog expects to interact with ES6 and the index prefix is equal to `graylog_seq_db`.
```bash
  GRAYLOG_ELASTICSEARCH_VERSION: "6"
  GRAYLOG_ELASTICSEARCH_INDEX_PREFIX: graylog_seq_db
```
2) *After* launching Graylog index ranges should be recalculated once.  
  To do this, enter *Graylog UI -> System -> Indices -> Default Index Set -> Maintenance*, then click *Recaculate index ranges*.

In case anything works wrong, check out the [Troubleshooting](#troubleshooting) section.

## Supported features
Here's a list of features in Graylog that we currently provide support for:
- Basic search/store (See [Ingestor API](./ingestor-api.md))
- Autocomplete
- Export to CSV
- `/api/search/universal/absolute` and `/api/search/universal/relative` handlers.

The sections below contains some insights and things we had to change in our API to be able to enable the features described above.

### Autocomplete
In order to enable autocomplete, a list of mappings should be provided when launching SeqDB, using the `--mapping` flag.
The provided mapping is analyzed by SeqDB and returned in the `/graylog_seq_db_0/_mapping` handler requested by Graylog.  

This mapping however won't be accessed and used unless Graylog is sure:
  1) There's an index alias for the default index (`graylog_seq_db` - in our case. This is why we needed this value for the `GRAYLOG_ELASTICSEARCH_INDEX_PREFIX` environment variable). The hardcoded aliases are returned by the `/_all/_alias/graylog_seq_db_deflector` and `/graylog_seq_db_0/_alias` handlers.
  2) The indices found in the previous step are opened. This is handled by the `/_cat/indices/graylog_seq_db_0` URL, that basically always says that the `graylog_seq_db_0` index `open`ed.
  3) The index range is calculated correctly. More about this in the [Troubleshooting](#troubleshooting) section.

### Export to CSV
When someone tries to export their logs to a CSV file (it can be done in a ton of different ways) a POST request with search parameters is made to one of this handlers:
  - `/_search/{store_name}`
  - `/{store_name}/message/_search`  

Our first guess was that any URL containing `_search` should return a regular list of search results (just like `/_msearch` does), but then we found that these two URL patterns are the most commonly used ones, so we decided to use them.  
The `_search` word is however still searched for in requests not matching any of the known URL patterns.
On request to this handler SeqDB returns the list of results used to build the CSV export file, logging an `unhandled csv export url path` error. 

### `/api/search/universal/absolute` and `/api/search/universal/relative`
This feature works okay if the index ranges were calculated correctly.

### Note: ES version
Graylog index recalculation doesn't work unless it has a 'healthy' connection to an ES cluster. The term 'healthy' means that the `ES_ip/` returns a JSON with the ES version. That's why the `/` handler was added to the SeqDB proxy, it returns a json with some info about the cluster (name and version).


## Troubleshooting
## MongoDB
After you have done all the steps described in the [Setup & Run](#setup-run) section, you can check if all the changes were correctly commited to the MongoDB database. Here's a small list of collections to look at:
- `index_ranges`: after index range recalculation it should contain a record looking similar to this:
  ```json
  {
    "_id": { 
      "$oid": "624d86e6e402186e6119d879"
    },
    "index_name": "graylog_seq_db_0",
    "took_ms": 0,
    "end": {
      "$numberLong": "0"
    },
    "calculated_at": {
      "$numberLong": "1649247974641"
    },
    "begin": {
      "$numberLong": "0"
    }
  }
  ```
- `index_field_types` should contain at least one object that looks like this:
  ```json
  {
    "_id": {
      "$oid": "624d94ae63fc90d6887ebe49"
    },
    "index_set_id": "5bbf2a614cedfd0001472aed",
    "index_name": "graylog_seq_db_0", // index prefix
    "fields": [
      {
        "field_name": "call_keyword_start",
        "physical_type": "keyword"
      },
      {
        "field_name": "handler",
        "physical_type": "keyword"
      },
      // ... along with other keywords
    ]
  }
  ```

- The `Default index set` record in the `index_sets` collection should have the following `index_prefix`: `graylog_seq_db`. In case you have an already working deployment that has a different index prefix, changing the `index_prefix` to the correct one shouldn't break anything. The described features **won't work** if the `index_prefix` for the `Default Index Set` is wrong.

