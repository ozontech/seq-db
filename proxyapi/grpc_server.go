package proxyapi

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Register gzip compressor
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/network/grpcutil"
	"github.com/ozontech/seq-db/network/ratelimiter"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/tracing"
)

type grpcServer struct {
	server *grpc.Server
}

func newGRPCServer(
	apiConfig APIConfig,
	si *search.Ingestor,
	mp MappingProvider,
	rl *ratelimiter.RateLimiter,
	mirror seqproxyapi.SeqProxyApiClient,
) *grpcServer {
	s := initServer()

	apiV1 := newGrpcV1(apiConfig, si, mp, rl, mirror)
	seqproxyapi.RegisterSeqProxyApiServer(s, apiV1)
	// Expose store API so aggregator proxies can query this proxy as a store (proper aggregations e.g. quantiles).
	storeapi.RegisterStoreApiServer(s, newStoreEmulator(si))

	return &grpcServer{
		server: s,
	}
}

func initServer() *grpc.Server {
	interceptors := []grpc.UnaryServerInterceptor{
		grpcutil.TraceUnaryInterceptor(),
		grpcutil.RecoverUnaryInterceptor(),
		grpcutil.LogUnaryInterceptor(),
		grpcutil.ReturnToVTPoolUnaryServerInterceptor(),
	}
	streamInterceptors := []grpc.StreamServerInterceptor{
		grpcutil.RecoverStreamInterceptor(),
		grpcutil.LogStreamInterceptor(),
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(interceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
		grpc.StatsHandler(&tracing.ServerHandler{}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     time.Minute * 2,
			MaxConnectionAge:      time.Minute * 10,
			MaxConnectionAgeGrace: time.Second * 30,
			Time:                  time.Minute * 5,
			Timeout:               time.Second * 20,
		}),
	}

	s := grpc.NewServer(opts...)
	reflection.Register(s)
	return s
}

func (s *grpcServer) Start(lis net.Listener) {
	addr := lis.Addr().String()

	logger.Info("ingestor grpc listening started", zap.String("addr", addr))

	if err := s.server.Serve(lis); err != nil {
		logger.Fatal("ingestor failed to serve grpc", zap.String("addr", addr), zap.Error(err))
	}
}

func (s *grpcServer) Stop(ctx context.Context) {
	grpcutil.StopGRPCServer(ctx, s.server)
}
