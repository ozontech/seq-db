package grpcutil

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ozontech/seq-db/logger"
)

// StopGRPCServer tries to gracefully stop the given gRPC server.
// If the context is canceled before graceful stop is finished,
// applies force stop.
func StopGRPCServer(ctx context.Context, grpcServer *grpc.Server) {
	stopCtx, cancel := context.WithCancel(ctx)
	go func() {
		grpcServer.GracefulStop()
		cancel()
	}()

	<-stopCtx.Done()

	if stopCtx.Err() == context.Canceled {
		logger.Warn("grpc server gracefully stopped")
	} else {
		logger.Error("failed to gracefully stop grpc server", zap.Error(stopCtx.Err()))
		logger.Warn("grpc server force stop")
		grpcServer.Stop()
	}
}
