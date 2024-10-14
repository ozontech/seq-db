# Setup

Utility functions useful for setting tests up

# Doc 

Functions for generating documents for tests and benchmarks. 

You can generate random data, or fill it by hand, and then convert them to jsons to pass for bulk/check search results

# Env

Functions for env config and creating the nodes from config

# Methods

Useful methods for calling ingestor/store handles

* `Bulk`/`BulkBuffer`/`GenBuffer`/...: generates buffer from docs and send them to bulk handle. Also asserts, that everything went well
* `Search`/`SearchHTTP`: convenient ways to search
