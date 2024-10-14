package debugserver

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type Server struct {
	server *http.Server
}

func New(httpAddr string, ready *atomic.Bool) Server {
	return Server{
		server: &http.Server{
			Addr:    httpAddr,
			Handler: initHandler(ready),
		},
	}
}

func (s Server) Start() {
	logger.Info("debug listen started", zap.String("addr", s.server.Addr))
	err := s.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal("failed to start listen on debug addr", zap.Error(err))
	}
}

func (s Server) Stop(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := s.server.Shutdown(ctx)
	if err == nil {
		logger.Info("shutdown debug server successful")
	} else {
		logger.Error("shutdown debug server", zap.Error(err))
	}
}

func initHandler(ready *atomic.Bool) http.Handler {
	mux := http.DefaultServeMux

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/live", liveness)
	mux.HandleFunc("/ready", readiness(ready))
	mux.Handle("/log/level", logger.Handler())

	return mux
}

// liveness is always ok
func liveness(w http.ResponseWriter, req *http.Request) {
	defer func() {
		_ = req.Body.Close()
	}()
	_, err := w.Write([]byte("OK"))
	if err != nil {
		logger.Error("failed to write liveness status", zap.Error(err))
	}
}

// readiness returns OK when service started
func readiness(ready *atomic.Bool) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			_ = req.Body.Close()
		}()
		var err error
		if ready.Load() {
			_, err = w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte("Not ready"))
		}
		if err != nil {
			logger.Error("failed to write readiness status", zap.Error(err))
		}
	}
}
