# Sharding and replication
seq-db comes with replication features out of the box, providing fault tolerance 
data availability using copies of data across multiple nodes and even datacenters.

This document describes the available seq-db replication features as well the 
replication model used in seq-db.

## seq-db proxy and replication
Because seq-db stores act as standalone stateful applications, 
the only component aware of the replication factor and cluster architecture 
is the seq-db proxy. 

The default replication factor used in seq-db proxy is `rf=1`.
The replication factor can be changed using the `--replicas` and `--hot-replicas` flag 
for the cold and hot stores respectively. Read more about hot-cold architecture here <!-- todo -->.


## Read and write semantics
seq-db proxy only supports synchronous replication, meaning that a write operation 
is only successful when seq-db proxy receives an acknowledgement 
from **all** replicas that the received documents were persisted on disk.

When reading from a shard, on the other hand, a response from one of the 
replicas of a specific shard is enough to form a search response. 