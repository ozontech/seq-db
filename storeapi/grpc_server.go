package storeapi

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Register gzip compressor
	"google.golang.org/grpc/keepalive"

	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/network/grpcutil"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/tracing"
)

type grpcServer struct {
	server *grpc.Server

	apiV1 *GrpcV1
}

func newGRPCServer(cfg APIConfig, fracManager *fracmanager.FracManager, mappingProvider MappingProvider) *grpcServer {
	s := initServer()

	apiV1 := NewGrpcV1(cfg, fracManager, mappingProvider)
	storeapi.RegisterStoreApiServer(s, apiV1)

	return &grpcServer{
		server: s,
		apiV1:  apiV1,
	}
}

func initServer() *grpc.Server {
	interceptors := []grpc.UnaryServerInterceptor{
		grpcutil.ReturnToVTPoolUnaryServerInterceptor(),
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(interceptors...),
		grpc.MaxRecvMsgSize(int(units.MiB) * 256),
		grpc.MaxSendMsgSize(int(units.MiB) * 256),
		grpc.StatsHandler(&tracing.ServerHandler{}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     time.Minute * 2,
			MaxConnectionAge:      time.Minute * 10,
			MaxConnectionAgeGrace: time.Second * 30,
			Time:                  time.Minute * 5,
			Timeout:               time.Second * 20,
		}),
	}

	return grpc.NewServer(opts...)
}

func (s *grpcServer) Start(lis net.Listener) {
	logger.Info("store grpc listening started", zap.String("addr", lis.Addr().String()))
	err := s.server.Serve(lis)
	if err != nil {
		logger.Fatal("store failed to serve grpc", zap.Error(err))
	}
}

func (s *grpcServer) Stop(ctx context.Context) {
	grpcutil.StopGRPCServer(ctx, s.server)
}
