package storeapi

import (
	"context"
	"fmt"
	"net"
	"path"

	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/compaction"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/skipmaskmanager"
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

	FracManager     *fracmanager.FracManager
	fracManagerStop func()

	SkipMaskManager    *skipmaskmanager.SkipMaskManager
	CompactionExecutor *compaction.Executor

	isStopped atomic.Bool
}

type StoreConfig struct {
	API                   APIConfig
	FracManager           fracmanager.Config
	SkipMaskManagerConfig skipmaskmanager.Config
	Compaction            compaction.Config
}

func (c *StoreConfig) setDefaults() error {
	if err := c.API.setDefaults(); err != nil {
		return err
	}
	if c.API.Search.Async.DataDir == "" {
		c.API.Search.Async.DataDir = path.Join(c.FracManager.DataDir, "async_searches")
	}
	if c.SkipMaskManagerConfig.DataDir == "" {
		c.SkipMaskManagerConfig.DataDir = path.Join(c.FracManager.DataDir, "skipmasks")
	}
	return nil
}

func NewStore(
	ctx context.Context,
	c StoreConfig,
	s3cli *s3.Client,
	mappingProvider MappingProvider,
	skipMaskParams []skipmaskmanager.SkipMaskParams,
) (*Store, error) {
	if err := c.setDefaults(); err != nil {
		return nil, err
	}

	skipMaskManager := skipmaskmanager.New(ctx, c.SkipMaskManagerConfig, skipMaskParams, mappingProvider)
	fracManager, stop, err := fracmanager.New(ctx, &c.FracManager, s3cli, skipMaskManager)
	if err != nil {
		return nil, fmt.Errorf("loading fractions error: %w", err)
	}

	planner := compaction.NewPlanner(ctx, fracManager, c.Compaction)
	executor := compaction.NewExecutor(c.Compaction.Workers, c.FracManager.SealParams, planner)

	skipMaskManager.Start(fracManager)

	return &Store{
		Config: c,
		// We will set grpcAddr later in Start()
		grpcAddr:           "",
		grpcServer:         newGRPCServer(c.API, fracManager, mappingProvider),
		FracManager:        fracManager,
		fracManagerStop:    stop,
		SkipMaskManager:    skipMaskManager,
		CompactionExecutor: executor,
		isStopped:          atomic.Bool{},
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
	s.fracManagerStop()
	s.SkipMaskManager.Stop()
	s.CompactionExecutor.Stop()

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
	s.FracManager.WaitIdleForTests()
}

func (s *Store) SealAll() { // tests only
	s.FracManager.SealForcedForTests()
}
