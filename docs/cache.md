# Cache overview

Currently, cache system's responsibility is to optimize access to index on disk
by storing in memory recently accessed data, as to avoid excess reads.
Data is stored in most cases in near-raw format, just after unpacking.

Core concepts:
- `cache.Cache`: single cache instance, mimicking generic map with uint32 keys;
- `cache.bucket`: interface, implemented by `Cache` and used by `Cleaner`;
- `cache.Cleaner`: keeps collection of `bucket`s, implements cleaning
- layer: type of data we are to cache. There's six different layers;
- `frac.SealedIndexCache`: one for a fraction. Keeps together `Cache` instances
  for this fraction, one per layer;
- `fracmanager.CacheMaintainer`: keeps all `Cleaner`s, is responsible
  for maintaining size, logging and creating new `SealedIndexCache`s.

Overall, there are two parallel layouts.

First, every fraction has its own `SealedIndexCache`, which holds reference
to all related `Cache` instances, allowing fraction (and its substructures)
access to cache, and to stored on disk index through it.

Second, `FracManager` holds reference to `CacheMaintainer` which performs all
the maintenance through respective `Cleaner`s. Each `Cleaner` not necessarily
represent single layer, though every layer is maintained by one `Cleaner`.

Package `cache` is implemented independently of other packages,
with encapsulation in mind.

On the other hand, `SealedIndexCache` and `CacheMaintainer` bind it to
our layers, limits and workflow.

# Cache layers

See [Index File Format](./format-index-file.md).

There are 7 cache layers:
- `index`: also called `Registry`, contains information on the fraction index
  file layout;
- `tokens`: all the tokens from all the indexed fields in docs in fraction;
- `lids`: for every token list of lids that match it;
- `docsdocblock`: for docblocks read during Fetch;
- `mids`, `rids`, `params`: mid, rid and position (docs block index, doc offset)
  of every lid, allowing to identify and locate corresponding doc.

`Cache` instances related to same layer are maintained by the same `Cleaner`.
There are currently 5 of them in `CacheMaintainer`:
- `lids` cleaner: this one is responsible for instances related to `lids` layer;
- `tokens` cleaner: same for `tokens` layer;
- `index` cleaner: same for `index` layer;
- `docblock` cleaner: same for `docblock` layer
- `mids-rids-params` cleaner: this one takes care of other layers, i.e. `mids`, `rids` and `params`.

They are separated this way due to their relative loads. `lids` have very big
values and are accessed often. `tokens` instead have very big amount of small
values, and are accessed often too. Other layers do not match, and thus can be
combined as to provide some basic level of protection against being purged by
`lids` or `tokens`, which in turn require protection from each other.

### Layer sizes

Layers don't have prefixed sizes. Instead, each `Cleaner` is associated with
size restriction. Restrictions are measured in fraction of total cache size:
- `lids`: 40%
- `tokens`: 40%
- `docblock`: 8%
- `index`: 3%
- `mids-rids-params`: 9%

# Interface

`Cache` has one basic access method, that being `Get`.
It takes the key and the factory function that will be called to create
new value if the key is not present. The value type is generic.

The second access method is `GetWithError`. This one allows factory function
to return error instead of value, which will not be saved in cache,
but passed through.

`Cache` can store any type of values inside, and `SealedIndexCache`
holds the exact instances, specialized for layer-related structures.

# Internals

Current implementation has one `mu sync.Mutex` for each `Cache` instance,
serving as a global lock. Under protection of this lock atomic operations
are performed, accessing and modifying the data.

Simple access to cache is designed to take lock only for brief periods of time.
Some other whole-cache-related operations, like getting stats or cleaning,
are allowed to take lock for longer times, given this doesn't trouble
simple accesses.

In most cases access requires only one such period. Adding new value
to the cache takes two though. Creation of the corresponding value takes time,
and thus we only need lock in the beginning and in the end. Rarely,
in case of panics (or returned error) in provided factory function,
it can take more lock/unlock cycles.

Data is stored in `entry` objects, which are referenced from
`payload map[uint32]*entry[V]`. Entries are organized into generations as to
facilitate cache cleaning process. `generations []*Generation` are thus
stored in `Cleaner` instances for this purpose.

### Entries

Internally, cache operates on `entry` objects, which represent stored
`value V` with some cache-related information:
- `wg *sync.WaitGroup`. This one is of most importance, as it is used to
  wait for value creation, and indicates whether it was successful.
  It's initialized with `Add(1)`.
- `gen *generation` links entry to its generation.
  `nil` until `value` is loaded. The only one that keeps changing and
  cannot be accessed without lock even when `wg` is `nil` or waited on.
- `size` contains size of the loaded value.

All entry changes are done under `Cache.mu` lock, and between locks
each entry can be in one of the following states:
1. Not present. There's no value nor entry for this key,
   as it wasn't accessed since cache creation, or was cleared.
2. Entry is present, `wg` is not `nil`, but `Done()` wasn't yet called.
   It means that this key was recently accessed,
   and the value for it is being created right now.
   There may be some pending accesses that wait on `wg`.
   `gen` and `size` are not initialized.
3. Entry is present, `wg` is `nil`.
   This means the entry is valid, and `value` can be returned.
   `gen` and `size` have correct values.
4. Entry has `wg`, `Done()` was already called.
   This indicates that entry is invalid. In this case it's not present
   in `payload` map and exists only because someone may be waiting on it.
   Entry gets into this state if the function responsible for value creation
   has panicked, or returned error. The entry was then marked as invalid
   and removed, and is to be created again by someone else.
   `gen` and `size` are both not initialized.
   Everyone waiting on this `wg` should reattempt access, possibly creating
   new entry themselves.

### Generations

For the cleaning purposes, all entries are linked to some generation,
which was stored in `Cache.currentGeneration` at the moment of such `entry`
becoming valid or last accessed, whichever happens later.
Entries that are not yet valid don't have generation.

All generation-related operations are performed under `Cache.mu` lock.
Between locks, all `generation` objects are in one of the three states:
- Current generation. There's always one and only one such generation.
  That one is stored in every accessed `entry`.
- Non-current and non-stale generation. Most generations are in this state.
  The values linked to these generations were accessed recently enough to
  be protected from clearing.
- Stale. This state is for generations that are considered old, but were not yet
  cleaned because they weren't considered big enough.

### Cleaning

Cleaning consists of two independent process. First one is periodical creation
of new generations, and the second one is periodical marking generations
as stale and consequent cleaning of them.

Each `Cleaner` instance has set of generations. New generations are created
`Cleaner`-wide when some known amount (say, 10% of the limit) of new data was stored.
At this moment every `Cache` instance is instructed to use new generation as current,
thus sealing the last one.

When the `Cleaner` decides that it has started to exceed its memory limit, it marks the
oldest few generations, which together are large enough to free up memory, as stale.
It then initiates the removal of all stale generations entries from each `Cache`.

Also time to time `Cleaner` finds released `Cache`s and removes it from list as well
as empty generations.

# Metrics

There are several metrics collected as to check cache health.
Each metric is collected separately for each layer.
- HitsTotal - access counter, when value was present
- MissTotal - access counter, when value was inserted by accessor
- MissLatency - counter of seconds me spend getting data when we miss cache
- PanicsTotal - access counter, when value wasn't inserted by accessor
- LockWaitsTotal - number of times initial lock for simple access
  wasn't acquired immediately, thus forcing rescheduling
- WaitTotal - number of times access method had to wait for someone else to
  put value into cache
- ReattemptsTotal - number of times access method had to reattempt due to entry
  invalidation
- SizeOccupied - counter of size of the values added to cache on miss
- SizeRead - counter of size of the values retrieved on hit
-	SizeReleased - counter of size of data we released
- MapsRecreated - counter of events we recreate cache maps

As a rule, `TouchTotal = HitsTotal + MissTotal + PanicsTotal`.
Also `TotalSize = SizeOccupied - SizeReleased`.
MissTotal and SizeOccupied are the metrics to optimize cache.
HitsTotal and SizeRead are the metrics to optimize cache use.
Rising WaitTotal means concurrent access to the same not-yet-cached value.

LockWaitsTotal ideally should be near-zero at all times and
shows cache internal efficiency in terms of concurrent access. This is the one
to check when implementing whole-cache-related operations.
PanicsTotal and ReattemptsTotal ideally should be near-zero at all times and
show problems with cache usage.