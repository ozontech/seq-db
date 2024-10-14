#!/bin/bash

set -euxo pipefail

curl "localhost:9002/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
{"index":{"_index":"index-other","_type":"span"}}
{"message": "world", "kind": "normal"}
{"index":{"_index":"index-main","_type":"span"}}
{"message": "beautiful", "kind": "old"}
{"index":{"_index":"index-other","_type":"span"}}
{"message": "place", "kind": "old"}
{"index":{"_index":"index-main"}}
{"message": "some message 1", "component": "A"}
{"index":{"_index":"index-main"}}
{"message": "some message 2", "component": "A"}
{"index":{"_index":"index-main"}}
{"message": "some message 3", "component": "B"}
{"index":{"_index":"index-main"}}
{"message": "some message 4", "component": "B"}
{"index":{"_index":"index-main"}}
{"message": "some message 5", "component": "B"}
{"index":{"_index":"index-main"}}
{"message": "some message 6", "component": "C"}
{"index":{"_index":"index-main"}}
{"message": "some message 7", "component": "C"}
{"index":{"_index":"index-main"}}
{"message": "some message 8", "component": "C"}
{"index":{"_index":"index-main"}}
{"message": "some message 9", "component": "C"}
{"index":{"_index":"index-main"}}
{"message": "some message 10", "component": "A"}
{"index":{"_index":"index-main"}}
{"message": "some message 11", "component": "B"}
{"index":{"_index":"index-main"}}
{"message": "some message 12", "component": "C"}
{"index":{"_index":"index-main"}}
{"message": "prefix1_blabla", "component": "A"}
{"index":{"_index":"index-main"}}
{"message": "prefix1_blabla", "component": "B"}
{"index":{"_index":"index-main"}}
{"message": "prefix2_blabla", "component": "C"}
'
