# Tests

## Usage

To run all tests: `go test ./...`

To run all tests inside `tests`: `go test ./tests/...`

You can use `-run` to filter: `-run "Suite/Env/Test`:
* To run specific test suite: `go test ./tests/... -run "SomeTestSuite/.*/.*`
* To run specific test suite with specific env: `go test ./tests/... -run "SomeTestSuite/SomeEnv/.*`
* To run specific test: `go test ./tests/... -run ".*/.*/SomeTest`
* To run specific test with specific env: `go test ./tests/... -run ".*/SomeEnv/SomeTest`

## Docs

There are some readmes inside folders

## tmp

Folder with data created during tests. You can safely remove it

## data

Folder with data needed to run tests

