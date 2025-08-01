package storeapi

import (
	"context"
	"fmt"
	"net"
	"path"

	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/storage/s3"
)

const (
	StoreModeHot  = "hot"
	StoreModeCold = "cold"
)

type Store struct {
	Config StoreConfig

	grpcAddr   string
	grpcServer *grpcServer

	FracManager *fracmanager.FracManager

	isStopped atomic.Bool
}

type StoreConfig struct {
	API         APIConfig
	FracManager fracmanager.Config
}

func (c *StoreConfig) setDefaults() error {
	if err := c.API.setDefaults(); err != nil {
		return err
	}
	if c.API.Search.Async.DataDir == "" {
		c.API.Search.Async.DataDir = path.Join(c.FracManager.DataDir, "async_searches")
	}
	return nil
}

func NewStore(ctx context.Context, c StoreConfig, s3cli *s3.Client, mappingProvider MappingProvider) (*Store, error) {
	if err := c.setDefaults(); err != nil {
		return nil, err
	}

	fracManager := fracmanager.NewFracManager(ctx, &c.FracManager, s3cli)
	err := fracManager.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading time list: %s", err)
	}
	fracManager.Start()

	return &Store{
		Config: c,
		// We will set grpcAddr later in Start()
		grpcAddr:    "",
		grpcServer:  newGRPCServer(c.API, fracManager, mappingProvider),
		FracManager: fracManager,
		isStopped:   atomic.Bool{},
	}, nil
}

func (s *Store) Start(lis net.Listener) {
	s.grpcAddr = lis.Addr().String()

	go s.grpcServer.Start(lis)

	metric.StoreReady.Inc()

	logger.Info("store started")
}

func (s *Store) Stop() {
	if s.isStopped.Swap(true) {
		return // already stopped
	}

	ctx, cancel := context.WithTimeout(context.Background(), consts.GRPCServerShutdownTimeout)
	defer cancel()

	s.grpcServer.Stop(ctx)

	s.FracManager.WaitIdle()
	s.FracManager.Stop()

	logger.Info("store stopped")
}

func (s *Store) GrpcAddr() string {
	if s.grpcAddr == "" {
		panic("bug: store not started")
	}
	return s.grpcAddr
}

func (s *Store) GrpcV1() *GrpcV1 { // tests only
	return s.grpcServer.apiV1
}

func (s *Store) WaitIdle() { // tests only
	s.FracManager.WaitIdle()
}

func (s *Store) SealAll() { // tests only
	s.FracManager.SealForcedForTests()
}

func (s *Store) ResetCache() { // tests only
	s.FracManager.ResetCacheForTests()
}
