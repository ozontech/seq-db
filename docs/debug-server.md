# Debug server

On a debug port there is an HTTP server for following purposes:

* `/metrics` Prometheus metrics endpoint
* `/live` Kubernetes liveness probe
* `/readiness` Kubernetes readiness probe
* `/log/level` zap logger handler, see go.uber.org/zap@v1.18.1/http_handler.go

Port can be configured via config, by default it is `9200`.
