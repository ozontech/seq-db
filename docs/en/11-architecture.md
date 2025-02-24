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
seq-db proxies act as the coordinators of all search and 
write operations and can be scaled independent of the seq-db stores.

seq-db proxy contains a list of stores written to and read from, 
and can be modified by the `--hot-stores`, `--hot-read-stores` for the hot replicas 
and `--read-stores`, `--write-stores` for the cold replicas. 

We call a set of one or more replicas used in the seq-db proxy configuration a **shard**.

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


## What's next
- Take a look at how seq-db handles replication
- Learn how to set up hot-cold storage using seq-db proxy and stores
