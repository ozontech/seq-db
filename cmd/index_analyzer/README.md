# seq-db Index Analyzer

To run|for example:

```
> go run ./cmd/index_analyzer/... ./data/*.index | tee ~/report.txt
```

if you have a set of index filed in ./data/ directory

As result you will get a set of statistic data in CSV format

## Uniq Tokens Stats

|N|docs|docsSum|fields|tokens|tokensSum|uniqTokensSum|tokenSize|tokenSizeSum|uniqTokenSizeSum|
|-|----|-------|------|------|---------|-------------|---------|------------|----------------|
|1|1021281|1021281|455|6370602|6370602|6370602|100973051|100973051|100973051|
|2|1002724|2024005|469|6378849|12749451|11806947|100243436|201216487|189662176|

Each row contains data of next processed fraction index.
 
- docs - Number of documents in fraction
- docsSum - Cumulative total number of documents 
- fields - Cumulative total number of fields
- tokens - Number of tokens in fraction
- tokensSum - Cumulative total number of tokens
- uniqTokensSum - Number of unique tokens in fractions for this table row and above
- tokenSize - Total size of tokens in fraction (bytes)
- tokenSizeSum - Cumulative total size of tokens
- uniqTokenSizeSum - Total size of all uniq tokens in fractions for this table row and above

## LIDs Histogram

|N|1|2|4|8|16|32|64|128|256|512|1024|2048|4096|8192|16384|32768|65536|131072|262144|524288|1048576|2097152|4194304|8388608|16777216|
|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
|1|5066713|665965|319099|139046|72002|33557|24001|15789|10758|11128|5067|3065|1684|1384|798|293|147|70|11|5|20|0|0|0|0|
|2|9413836|1234627|612087|254150|118411|66178|33443|25276|14643|10928|10917|5066|3051|1614|1380|794|297|147|66|11|5|20|0|0|0|

The table represents the distribution|where
- in the table header -- the bucket size (number of documents)
- the first column -- the number of fractions
- in each cell -- the number of tokens that occur in the corresponding number of documents (shown in the header)

## Tokens Histogram

|N|1|2|4|8|16|32|64|128|256|512|1024|2048|4096|8192|16384|32768|65536|131072|262144|524288|1048576|2097152|4194304|8388608|16777216|
|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
|1|78|33|26|49|45|36|28|27|29|36|14|11|16|11|4|1|4|1|1|0|2|3|0|0|0|
|2|72|26|36|32|52|38|31|30|25|37|29|8|15|15|6|5|1|4|1|1|0|3|2|0|0|

The table represents the distribution|where
- in the table header -- the bucket size (number of tokens)
- the first column -- the number of fractions
- in each cell -- the number of fields with corresponding number of tokens (shown in the header)

## Uniq LIDs Stats

Uniq LIDs Stats
 N|docs|lidsCount|lidsUniqCount|
 -|-|-|-|
 1|1021281|90613582|69135909|
 2|1002724|88643738|67335634|
 3|1049143|91475838|69721189|
 4|1018191|89420909|68159272|

 Table shows difference of size full LIDs section and size only uniq LIDs lists