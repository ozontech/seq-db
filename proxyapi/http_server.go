package proxyapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type httpServer struct {
	server *http.Server
}

func newHTTPServer(handler http.Handler) *httpServer {
	return &httpServer{
		server: &http.Server{
			Handler: handler,
		},
	}
}

func (s *httpServer) Start(listener net.Listener) {
	// Resolve addrs like ":"
	addr := listener.Addr().String()

	logger.Info("ingestor http listening started", zap.String("addr", addr))

	if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal("ingestor can't listen http addr", zap.String("http_addr", addr), zap.Error(err))
	}
}

func (s *httpServer) Stop(ctx context.Context) {
	if err := s.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("ingestor http server graceful shutdown error", zap.Error(err))
	} else {
		logger.Warn("ingestor http server gracefully stopped")
	}
}

type ingestorHandler struct {
	esVersion   string
	bulk        http.Handler
	grpcGateway http.Handler
}

func newIngestorHandler(esVersion string, bulk, grpcGateway http.Handler) *ingestorHandler {
	return &ingestorHandler{
		esVersion:   esVersion,
		bulk:        bulk,
		grpcGateway: grpcGateway,
	}
}

func (h *ingestorHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	if path == "/_bulk" {
		h.bulk.ServeHTTP(w, req)
		return
	}

	if strings.HasPrefix(path, "/_ilm/policy") ||
		strings.HasPrefix(path, "/_index_template") ||
		strings.HasPrefix(path, "/_ingest") ||
		strings.HasPrefix(path, "/_nodes") {
		// Return fake response for Filebeat/Logstash request.
		_, _ = w.Write([]byte(`{}`))
		return
	}

	switch path {
	case "/":
		if req.Method == http.MethodHead {
			// Return empty response for Logstash ping request.
			return
		}
		_, _ = fmt.Fprintf(w, `{"cluster_name": "seq-db","version": {"number": %q}}`, h.esVersion)
	case "/_license":
		_, _ = w.Write([]byte(`{"license":{"mode":"basic","status":"active","type":"basic","uid":"e76d6ce9-f78c-44ff-8fd5-b5877357d649"}}`))
	default:
		h.grpcGateway.ServeHTTP(w, req)
	}
}
