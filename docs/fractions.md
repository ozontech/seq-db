# Factions

## See also

 * **Fractions**
 * [Format of Documents and Metadata File](./format-docs-meta-file.md)
 * [Format of Index File of Sealed Fraction](./format-index-file.md) 

In terms of seq-db fraction is a data store unit. There are two types of fraction:

 * Active fraction. A fraction to which documents can be added. Documents and corresponding metadata are saved to disk, while an index of documents is stored in memory.
 * Sealed fraction. Immutable read-only fraction. When the size of an active fraction exceeds a certain threshold, its index is written to disk and the fraction becomes sealed.

### Active fraction

Active fraction consists of 2 files:

* Document file. It consists of a sequence of blocks. Each block contains all documents from the corresponding bulk request.
* Metafile. Like a document file, it consists of a sequence of blocks with document metadata from the corresponding bulk request.

The index to the documents on disk is stored in memory. In the event of an abnormal termination, seq-db reads the docfile and metafile from disk during service initialization and restores the index in memory.

This process of restoring the index in memory is called Replaying (see Active.ReplayBlocks() method)

In the event of a regular shutdown of the seq-db service, the active fraction is sealed if its size exceeds 20% of the maximum allowable size of the active fraction.

Regular sealing also occurs when the active fraction exceeds the maximum size. Then the active fraction is sealed and a new active fraction is created into which new incoming documents begin to be written.

### Sealed fraction

The sealed fraction consists of 2 files:

* Document file. The same as for the active faction.
* Index file. This is a serialized representation of the index of the active faction.

Sealing is the process of serializing the in-memory index of the active faction into an index file. 

The structure of the index file is complex and is described [here](./format-index-file.md) 

There is a mechanism for rotating sealed fractions. 

When the total dataset size exceeds the specified threshold, the oldest fractions will be removed to keep the dataset size within bounds.