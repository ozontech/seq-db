# seq-db architecture

## Single-node mode 
TBD

## Cluster mode
In case seq-db is running in a clustered environment - such as a Kubernetes cluster or any other distributed setup
it is possible to enable in cluster mode. 
Cluster mode allows for a flexible configuration that supports 
common use cases like hot-cold storage modes, replication factor settings etc.

When used in cluster mode, seq-db consists of two components,
determined by the `--mode` flag: 
- **seq-db proxy** - a stateless component responsible for indexing
the ingested documents and ensuring they are written to the chosen replicas. It also proxies all search requests and runs the external [ingestor API](06-ingestor-api.md).
- **seq-db store** - the stateful storage component that manages the data and the indices.
For a complete list of available configuration flags, refer to the [flags](02-flags.md) documentation.


## Cluster set up
seq-db proxy act as a coordinator for all search and 
write operations. In a clustered setup there can be multiple seq-db proxy instances scaled independent of the seq-db stores.

seq-db proxy contains a list of stores written to and read from, 
and can be modified by the `--hot-stores`, `--hot-read-stores` for the hot replicas 
and `--read-stores`, `--write-stores` for the cold replicas. The difference between the hot and cold replicas are described below.

We call a set of one or more replicas used in the seq-db proxy configuration a **shard**.


## hot-cold storage model
seq-db supports two types of seq-db stores out of the box: *hot* stores and *cold* stores. 

hot stores are used to stores frequently used data that needs to accessed quickly, while cold storages are used to store data that is not frequently accessed.

seq-db's cluster mode allows you to setup a cluster that has both hot and cold storages with different fault tolerance and availability guarantees in order to cost-effectively utilize your resources.

When both hot and cold shard are used, the seq-db proxy will first send the search query to the hot shards. If necessary, send that same  query to the cold storages  will be forwarded to the cold stores.


### Hot stores
Hot stores are managed using `--hot-stores` and  `--hot-read-stores` flags in seq-db proxy. Please, consult the [flags documentation](./02-flags) for information about the format accepted by these flags.

In order to be able to act as a hot store, a seq-db store shard should have the `--store-mode=hot` flag set. With this flag enabled, the seq-db store responds with a special code, that is used by seq-db proxy to know whether that same search query should be sent to the cold shards.

Note that the `--hot-stores` flag defines the stores used both to read to and write from, while the `--hot-read-stores` flag **overrides** the list of shards that are read from. In other words, the `--hot-read-stores` flag has a higher priority than the `--hot-stores` flag.

### Cold stores
Cold stores are managed using `--write-stores` and `--read-stores` flags in seq-db proxy.  Unlike the `hot-stores` and `hot-read-stores`, `read-stores` and `write-stores` have the same priority. That is, if no there are no `--read-stores` defined , the `--write-stores` will only be written to, and not read from. 

## Sharding and replication
seq-db comes with replication features out of the box, providing fault tolerance 
data availability using copies of data across multiple nodes and even datacenters.

This section describes the available seq-db replication features as well the 
replication model used in seq-db.

### seq-db proxy and replication
Because seq-db stores act as standalone stateful applications, 
the only component aware of the replication factor and cluster architecture 
is the seq-db proxy. 

The default replication factor used in seq-db proxy is `rf=1` on both hot and cold stores.
The replication factor can be changed using the `--replicas` and `--hot-replicas` flag 
for cold and hot stores respectively.

Important note: todo how replicas are grouped into shards based on values used in flags.


### Read and write semantics
seq-db proxy only supports synchronous replication, meaning that a write operation 
is only successful when seq-db proxy receives an acknowledgement 
from **all** replicas that the received documents were persisted on disk.

When reading from a shard, on the other hand, a response from one of the 
replicas of a specific shard is enough to form a search response. 


### Write semantics for the hot-cold model
Hot and cold replicas can have different replication factors.
In this case the write is going to be successful when **all hot and cold replicas** confirmed that the documents are persisted on disk. Therefore, in a situation when `--hot-replicas=2` and `--replicas=1`, the de-facto replication factor for the most recently written data is going to be equal to 3. After its deletion from the hot replicas, a single copy of the data will remain on the chosen cold replica.

### Read semantics for the hot-cold model
In case both hot and cold models are used, the read queries will, first of all, run on hot shards, and then on cold shards, if necessary.



<!-- 
## Quick local cluster setup

### Using the seq-db binary
You can easily set up a basic seq-db cluster on your machine using the compiled seq-db binary.
This setup will include a seq-db proxy and a seq-db store. 

Run the following command to start a seq-db store instance:
```bash
./seq-db --mode store 
         --mapping tests/data/mappings/tracing.yaml 
         --debug-addr :9201 
         --addr :9003
``` 

Start the seq-db proxy instance with:
```bash 
./seq-db --mode ingestor 
         --mapping tests/data/mappings/tracing.yaml  
         --hot-stores  localhost:9003
```


### Using docker 
<!--TBD-->

### Verify the setup
<!--TODO add few curl requests (cluster health, bulk, search)-->
 -->

## What's next
- Take a look at how seq-db handles replication
- Learn how to set up hot-cold storage using seq-db proxy and stores
