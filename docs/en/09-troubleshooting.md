---
id: troubleshooting
---

# Troubleshooting

This document provides guidance for troubleshooting common issues with resource usage and performance when working with
seq-db.

## Issue: High Memory Usage

If you notice that seq-db is consuming too much memory, consider the following actions:

### Reduce Cache Size

seq-db uses a cache to reuse data blocks previously read from disk. Reducing the cache size can help lower overall
memory usage. For more details on how caching works, refer to the [cache documentation](internal/cache.md).

### Adjust Go Runtime Settings

The Go runtime uses a garbage collector (GC) to free memory occupied by unused objects. By default, garbage collection
runs when the heap size doubles from the last GC run. You can configure the GC behavior with the following environment
variables:

* **`GOGC`** — specifies the target memory size after each GC run.
  For instance, if seq-db typically requires 1 GiB of memory (considering cache, internal buffers, and other resources),
  then with `GOGC=100` (the default value), memory usage may peak at 2 GiB.
  See [GOGC documentation](https://tip.golang.org/doc/gc-guide#GOGC) for more details.

* **`GOMEMLIMIT`** — sets the maximum memory size the application can use. When memory usage approaches this limit,
  garbage collection will run more frequently.
  For more details on `GOMEMLIMIT`, refer to [the documentation](https://tip.golang.org/doc/gc-guide#Memory_limit).

These parameters can be used together to optimize memory usage and CPU load. It is recommended to
review [this guide](https://tip.golang.org/doc/gc-guide#Memory_limit) to understand how to balance both settings
effectively.

To monitor the load created by the garbage collector, it's helpful to track the seq-db metric "The fraction of CPU time
used by the GC, %" (todo: не работает). It’s recommended that GC does not use more than 5% of
CPU time.

### Reduce the frac-size

The `frac-size` parameter defines the maximum amount of memory that seq-db will store before sealing the active
fraction. Reducing this parameter can help lower memory consumption but may increase query times. See more details
about `frac-size` in the [configuration documentation](02-flags.md#data-configuration-flags).

## Issue: Slow Search Queries

*todo*

## Issue: Slow Data Ingestion

*todo*
