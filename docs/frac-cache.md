# Fraction Cache (`.frac-cache`)

## Fractions, types of fractions
SeqDB stores the indexed data in special structures named *fractions*, each fraction is represented by two files on disk.

A fraction that is currently being written to is called an **active fraction**.  
A **sealed fraction** is a read-only fraction, that is not going to be modified by the database in any kind. A sealed fraction is only going to be read from.  

When an *active* fraction reaches a certain size (configured by the `frac-size` flag), the *active* fraction is turned into a *sealed* one, this process is named **sealing**. The database must have **only one active fraction** at any given time. (TBD link to sealing)

Both sealed and active fractions have the following *metadata*:
- `Name`  - fraction name
- `DocsTotal` - size of compressed docs stored on disk
- `DocsRaw` - size of raw docs stored on disk
- `MetaOnDisk`/`IndexOnDisk` - size of compressed index/meta data stored on disk
- `From` - the earliest document timestamp in fraction
- `To` - the latest document timestamp in fraction
- `CreationTime` - the time at which the fraction has been created

Please refer to the `Info` structure in the [`fraction.go`](../frac/fraction.go) file for more info on fraction metadata.

## Fraction cache for sealed fractions
SeqDB has a structure named **sealed fraction cache** containing sealed fractions metadata. This structure is stored in-memory and dumped to the disk each time a new fraction is sealed. 
The file that stores the dumped fraction cache is called `.frac-cache`, and is a JSON-marshaled list of sealed fraction metadata. 

In case of a restart/failure, the `.frac-cache` file is read, copied to the in-memory fraction cache structure, given that the fraction cache file is valid. A successful on-disk fraction cache load means we can skip fraction directory scanning & sealed fraction metadata reading. 

Note that in case of an invalid loaded fraction cache file, the metadata from the stored sealed fractions will be read once and the broken `.frac-cache` file will be updated. The `.frac-cache` file **must** only be edited by SeqDB, modifications made by any other user/program may screw things up.
