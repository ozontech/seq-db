# Suits

## Base

Base suite for every tests. It gives you your own test folder `s.DataDir`, that will not intersect with another tests, also clears it before and after test automatically 

It also can have env, but it isn't required: you can pass nil env and it will work

## Bench

Base suite for benchs. Sadly, testify doesn't have bench support, so there is our own implementation. See the code for how to include it correctly

## Single

Suite for simple tests with one ingestor and one store with default configs

In the beginning of the test you already have ingestor and store started, and you can get them as `s.Store`/`s.Ingestor`

There are also some convinient functions as 
* `s.Bulk`: convinient way to bulk
* `s.Search`/`s.SearchDocs`: convinient way to search
* `s.AssertDocsEqual`/`s.AssertSearch`: asserts, that `foundDocs` equal to docs from `originalDocs` with indexes `indexes` (see tests for examples)
* `s.RunFracEnvs`: runs function several times:
	* Active: just runs (implying that there is active fraction)
	* Sealed: seals, and then runs
	* Restarted: restarts store and then runs. This also seals, because we seal on restart
* `s.RestartStore`/`s.RestartIngestor`: (re)starts store/ingestor
* Feel free to add your own
