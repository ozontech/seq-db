package proxyapi

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ozontech/seq-db/network/grpcutil"
	"github.com/ozontech/seq-db/proxy/stores"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	storeapiclient "github.com/ozontech/seq-db/storeapi"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/network/ratelimiter"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/tracing"
)

type Ingestor struct {
	Config IngestorConfig

	httpServer *httpServer
	grpcServer *grpcServer

	BulkIngestor   *bulk.Ingestor
	SearchIngestor *search.Ingestor

	rateLimiter *ratelimiter.RateLimiter

	cancel context.CancelFunc

	isStopped atomic.Bool
}

func clientsFromConfig(ctx context.Context, config search.Config) (map[string]storeapi.StoreApiClient, error) {
	clients := map[string]storeapi.StoreApiClient{}
	if err := appendClients(ctx, clients, config.HotStores.Shards); err != nil {
		return nil, err
	}
	if config.HotReadStores != nil {
		if err := appendClients(ctx, clients, config.HotReadStores.Shards); err != nil {
			return nil, err
		}
	}
	if config.WriteStores != nil {
		if err := appendClients(ctx, clients, config.WriteStores.Shards); err != nil {
			return nil, err
		}
	}
	if config.ReadStores != nil {
		if err := appendClients(ctx, clients, config.ReadStores.Shards); err != nil {
			return nil, err
		}
	}
	return clients, nil
}

func appendClients(ctx context.Context, clients map[string]storeapi.StoreApiClient, shards [][]string) error {
	for _, shard := range shards {
		for _, replica := range shard {
			if _, has := clients[replica]; has {
				continue
			}
			// this doesn't block, and if store is down, it will try to reconnect in background
			conn, err := grpc.DialContext(
				ctx,
				replica,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStatsHandler(&tracing.ClientHandler{}),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					PermitWithoutStream: true,
					Time:                time.Minute * 2,
				}),
				grpc.WithConnectParams(grpc.ConnectParams{
					MinConnectTimeout: 100 * time.Millisecond,
					Backoff: backoff.Config{
						BaseDelay:  200 * time.Millisecond,
						Multiplier: 2,
						Jitter:     0.2,
						MaxDelay:   2 * time.Second,
					},
				}),
				grpc.WithUnaryInterceptor(grpcutil.PassMetadataUnaryClientInterceptor()),
			)
			if err != nil {
				return err
			}
			client := storeapi.NewStoreApiClient(conn)
			clients[replica] = client
		}
	}
	return nil
}

func NewIngestor(config IngestorConfig, store *storeapiclient.Store) (*Ingestor, error) {
	config.setDefaults()

	rateLimiter := ratelimiter.NewRateLimiter(config.API.QueryRateLimit, metric.RateLimiterSize.Set)

	ctx, cancel := context.WithCancel(context.Background())

	grpcGateway := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &humanReadableMarshaler{}),
	)
	err := seqproxyapi.RegisterSeqProxyApiHandlerFromEndpoint(ctx, grpcGateway, config.API.GatewayAddr,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("register grpc handler: %s", err)
	}

	var clients map[string]storeapi.StoreApiClient
	if store != nil {
		clients = map[string]storeapi.StoreApiClient{
			"memory": storeapiclient.NewClient(store),
		}
		config.Bulk.HotStores = stores.NewStoresFromString("memory", 1)
		config.Search.HotStores = stores.NewStoresFromString("memory", 1)
	} else {
		clients, err = clientsFromConfig(ctx, config.Search)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("initialize clients: %s", err)
		}
	}

	searchIngestor := search.NewIngestor(config.Search, clients)
	bulkClient := bulk.NewSeqDBClient(config.Bulk.HotStores, config.Bulk.WriteStores, config.Bulk.BulkCircuit, clients)
	bulkIngestor := bulk.NewIngestor(config.Bulk, bulkClient)

	var mirror seqproxyapi.SeqProxyApiClient
	if config.Search.MirrorAddr != "" {
		conn, err := grpc.NewClient(config.Search.MirrorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Error("failed to create mirror client", zap.Error(err))
		} else {
			mirror = seqproxyapi.NewSeqProxyApiClient(conn)
		}
	}

	handler := newIngestorHandler(config.API.EsVersion, NewBulkHandler(bulkIngestor, config.Bulk.MaxDocumentSize), grpcGateway)

	return &Ingestor{
		Config:         config,
		httpServer:     newHTTPServer(handler),
		grpcServer:     newGRPCServer(config.API, searchIngestor, config.Bulk.MappingProvider, rateLimiter, mirror),
		BulkIngestor:   bulkIngestor,
		SearchIngestor: searchIngestor,
		rateLimiter:    rateLimiter,
		cancel:         cancel,
		isStopped:      atomic.Bool{},
	}, nil
}

func (i *Ingestor) Start(httpListener, grpcListener net.Listener) {
	i.rateLimiter.Start()

	go i.httpServer.Start(httpListener)
	go i.grpcServer.Start(grpcListener)

	logger.Info("ingestor started")
}

func (i *Ingestor) Stop() {
	if i.isStopped.Swap(true) {
		panic(fmt.Errorf("ingestor already stopped"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), consts.GRPCServerShutdownTimeout)
	defer cancel()

	var wg sync.WaitGroup
	if i.grpcServer != nil {
		wg.Add(1)
		go func() {
			i.grpcServer.Stop(ctx)
			wg.Done()
		}()
	}
	wg.Add(1)
	go func() {
		i.httpServer.Stop(ctx)
		wg.Done()
	}()
	wg.Wait()

	i.BulkIngestor.Stop()
	i.rateLimiter.Stop()

	i.cancel()

	logger.Info("ingestor stopped")
}

// humanReadableMarshaler is used to replace runtime.JSONPb marshaler to json.Marshaler.
// It is used for human-readable output.
// See proxyapi.Document.MarshalJSON example.
// Used only for debug purposes in grpc-gateway.
type humanReadableMarshaler struct {
	runtime.JSONPb
	stdlibMarshaler runtime.JSONBuiltin
}

func (m humanReadableMarshaler) Marshal(v interface{}) ([]byte, error) {
	return m.stdlibMarshaler.Marshal(v)
}
