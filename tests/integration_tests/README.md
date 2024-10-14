# Integration tests

Main package with integration tests

# SingleTestSuite

If you need simple tests with one ingestor and one store with default configs, add them into SingleTestSuite

You can read about single suite in [tests/suits/README.md](../suits/README.md)

# IntegrationTestSuite

More general suite, it doesn't run the stores: you need to run them yourself.

This allows you to change default env/config to match your needs 

There are several envs, check them at [integration_sute.go:IntegrationEnvs()](integration_suite.go)

You can skip some of the envs with selecting them by `s.Config.Name` and `s.T().Skip`, see `TestSearchProxyTimeout` for an example

# ReplicasTestSuite

Suite to run tests with replicas

It starts the stores and ingestors before test

There are some convenient functions:
* `s.Bulk`: bulk store at random ingestor
* `s.SearchEachStore`: searches each store independently, and returns each store result

# EvalTestSuite

Special suite for eval test
