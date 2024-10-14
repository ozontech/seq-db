`id` – global id of the document across all seq-db cluster.

`lid` – local id of the document in the fraction (sealed and active).

`tid` - token id in the fraction (sealed and active).

`node` – [deprecated] actual term is `tid`.

`RC` – reader cell, struct which is used by a reader to read data from disk.

`PT` – [deprecated] prefix tree which is used to assign document IDs to terms, actual structure is `TokenList`.

`QPR` – query partial result