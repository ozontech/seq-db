# How searching works

> Read `parcing.md` before

Entrypoint is `Search` in `search.go`. We take an array of fractions and search them in `searchFrac` independently, and then merge the results. There's `searchFrac` and `searchFracImpl`: the first is an entrypoint and does some fraction initialization and takes locks, and the second is actual searching.

1. First we need to build an eval tree from AST: `BuildEvalTree` in `search/eval.go`. This is mostly 1 to 1 mapping of AST nodes, but with different types (to split functionality in different types). If a field query have several candidates (ranges or wildcards), it will build binary tree with all the candidates. Making this _binary_ tree saves a lot of time, so it is important. This tree's leaves implement node.Node interface.
  - The active fraction uses the node.nodeStatic implementation of node.Node, based on slice of LIDs in memory.
  - For sealed fraction lids.Iterator used. This iterator implements lazy load LIDs from disk/cache.
2. Also, if we need to build an aggregation, we build a similar OR tree for the query "agg_field: \*". Then, at the evaluation stage, we are getting the intersection of the received LIDs with the LIDs of the result of the main search query and build a map of counters with a token as a key.
3. Also we may add histograms, which is basically a map from time interval into how many documents are there.
4. Then we evaluate the tree, while there are some documents. We do this in "online" or "lazy", meaning that we don't evaluate each node fully, but read values one by one. It can heavily reduce the RAM usage during request. Values come in sorted order from each source, so we can do efficient operations on them.  
Some basic overview of nodes:
	* OR: works kinda like a merge sort: it reads an integer from both sources, and returns the smallest. If they are equal, it removes the duplicate.
	* AND: works similar to OR, but returns an element only if it's in both sources (== they are equal).
	* NOT: iterates over all documents and returns only that, which are _not_ in the source.
	* NAND (AND with one NOT): returns only the documents from regular source, that are not in the negative source. Reads all the negative skipping them, until we go above current regular; if they're equal, skip the regular, if not, we return it.

## Active mapping

> Reminder:
>
> **TID** (Token ID) - id of a field-value pair. Example: `service:auth-api` has tid = 3.
>
> **LID** (Local ID) - id of a document inside (local to) fraction.
>
> **ID** (document ID) - full id of a document, that you can use on proxy to find this specific doc. Consists of two parts: mid and rid.
>
> **MID** (milliseconds ID) - timestamp of a document. This is a timestamp, that log was written into stdout on the machine, not when it came into seq-db, meaning that it can be quite old relative to the time on the seq-db machine.
>
> **RID** (random ID) - random part of an id

In sealed fractions everything is sorted, meaning that lids are sorted by timestamp (mid) but also are in ascending order. But in active fraction that cannot be achieved, so `TokenList` returns an array of lids, which is sorted by timestamp (mid), but it isn't in ascending order.

For example, it can return an array `lids=[2, 1, 5, 0, 3]`. This means, that this is an order, in which documents are sorted by timestamp (i.e for this example mids could be `mids=[135, 137, 137, 138, 145]`). But our tree evaluation works only if all sources are sorted in ascending order. 

We could just sort the lids, but then the limit will not work: in our example, `limit=2` will give `[2, 1]`, but if we sort them, it will give `[0, 1]`.

So the idea is to compare not lids, but mids. We could write in each comparison something like

```go
// instead of
if leftID < rightID {...}

// this
if get_mids(leftID) < get_mids(rightID) {...}

// or even this
if !frac_active {
	if leftID < rightID {...}
} else {
	if get_mids(leftID) < get_mids(rightID) {...}
}
```

But we can do better. Because we need only 2 conditions from this ids: they are ordered and if ids are equal, that means it's the same document; we can do any 1 to 1 mapping on this ids, with condition that we will unmap them after evaluating.

You can imagine this as calling `get_mids` for every element in the array, and then working with that array instead of the original. But you also must hold original indexes, because mids can be equal for different documents.

So, we want some mapping from `lid` to `some_id`, in which the resulting array will be sorted.

Let's look at the `lids` for all documents in the fraction. For our example it can look something like: `all_lids=[2, 1, 4, 5, 0, 3]` (notice the `4` id). We can see, that this is an array from `0` to `len(docs)`, i.e this is a 1 to 1 permutation. Let's make a inverse permutation out of it, we'll get: `all_inv=[4, 1, 0, 5, 2, 3]`.

If we take our `all_lids` and apply `all_inv` mapping, we will get a sorted array from `0` to `len(docs)` (because that's the definition of inverse permutation). And we can notice, that when we get `lids` for some `tid`, we actually get some subsequence of this array. And if we apply mapping for subsequence, we'll still get a sorted array: `lids_mapped=[0, 1, 3, 4, 5]` (notice the missing `2`: it is an index for original `4` id, and we don't have it in the original `lids`).

And we can apply `all_lids` mapping to the array after inverse mapping and get the values back. Neat.

We can get `all_lids` from `_all_` field in seq-db, which contains all the documents. And then we can just build `all_ind` from it.

So the idea is this: (only for the active fraction) when we get `lids` for `tid`, we'll apply `all_inv` mapping, so we'll get a nice sorted array. Then we can do all the things in evaluating the tree. After this, we get this `inv_mapped_id` from the tree, so we'll map it with `all_lids`, and get the original `lid`.
