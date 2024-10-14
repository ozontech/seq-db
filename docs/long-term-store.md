# Introduction of long term stores

## Problem
Currently seq-db is using SSD storage to ensure good performance for users.
But SSD storage is limited, so we can't store a lot of historical data.
At the same time a small number of requests want to get historical data
for a long period of time.

## Solution
Natural solution to this is to introduce long term (cold) stores with 
large storage (possibly HDD). So, data should be written to both types of
stores. Most reads go to hot store, but reads for long periods should go
to long term stores.

## Stores
Ingestor knows several types of stores:
- hot stores (always used for write, used for search only if hot read stores are not enabled)
- hot read stores 
- long term (cold) stores (always used for write, used for search only if cold read stores are not enabled)
- long term (cold) read stores

### Read stores
Read mode of stores is needed for migration. 

Since write operation fails on single write failure it is necessary to exclude machine to be migrated from the write list.
It is done by enabling read stores (hot/cold respectively). If read stores are set, querying is done only through them and
regular stores continue to be used only for write.

Thus to move a regular (hot/cold) store `M` to another machine the pattern is:
- enable read stores (move all stores to be queried to the read list, including M)
- exclude M from regular list
- restart ingestor
- shutdown M. Since M is excluded from write, write operations will not fail
- migrate store
- disable read stores, return M to regular list
- restart ingestor

### Write
When data is written (bulk send), it is first sent to hot stores, then to cold stores. Error in writing to any of them results an overall error.
Currently data can be saved in long term store, but not in hot (TODO: fix).

### Querying
On search hot stores are queried first.

Hot stores refuse to search if `From` field is less than the oldest MID on this store
(it means search may ask for data that is already rotated on the store). Ingestor
receives an error and in case it has long term stores configured, queries them.
Both hot and cold store can return partial response, and it will be considered valid.
For now there are no error type checking because it is not trivial for GRPC,
this will be implemented in the future.

### Avoid old docs in hot store
There is a problem that a doc with very old timestamp may be submitted to hot store.
This doc will have very low seq.MID and sooner or later it will become the oldest
MID in the store. This will result a behavior that hot store will answer to wider
range of queries, when normally they should be sent to long term store.

To avoid this need to make an important change to bulk process. There is now a special check of time field
and three possible outcomes:
- Time field exists and has a correct value (not older than 24h from now):
  in this case no changes to doc and seq.MID calculated from this value.
- Time field does not exist: doc is not changed, seq.MID calculated
  from time.Now()
- Time field holds very old value (more than 24h from now): in this
  case doc is changed, value of time field is changed to time.Now(), seq.MID
  is calculated from that. Original timestamp is stored in field
  `original_timestamp`, this field is overwritten if exists.

## Deploy
As we don't have long term store right now, deploy will be done in several steps
to avoid interruption of service.

### First step
Current stores become hot stores, but hot mode is not enabled. In this case
behavior is the same as in older code. This removes an option to have read/write
stores separately, but it's necessary.

### Second step
This step may be done together with the first step. We create new stores with
large storage and add them to ingestors as write-stores. Now ingestors write
data to both types of stores, but read queries will go to hot stores only.
After that we need to wait for long term stores to have data at least for 
the same period as hot stores.

### Third step
Now we can add all long term stores as read stores and enable hot store-mode
for hot stores. This effectively enables a new scheme, when hot store can return
error and query will go to read (long term) stores. As before, write stores should
be a subset of read stores.
