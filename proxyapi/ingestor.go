package proxyapi

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ozontech/seq-db/config"
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

func clientsFromConfig(sConfig search.Config) (map[string]storeapi.StoreApiClient, error) {
	clients := map[string]storeapi.StoreApiClient{}
	if sConfig.HotStores != nil {
		if err := appendClients(clients, sConfig.HotStores.Shards); err != nil {
			return nil, err
		}
	}
	if sConfig.HotReadStores != nil {
		if err := appendClients(clients, sConfig.HotReadStores.Shards); err != nil {
			return nil, err
		}
	}
	if sConfig.WriteStores != nil {
		if err := appendClients(clients, sConfig.WriteStores.Shards); err != nil {
			return nil, err
		}
	}
	if sConfig.ReadStores != nil {
		if err := appendClients(clients, sConfig.ReadStores.Shards); err != nil {
			return nil, err
		}
	}
	if sConfig.RegionStores != nil {
		if err := appendClients(clients, sConfig.RegionStores.Shards); err != nil {
			return nil, err
		}
	}
	return clients, nil
}

func appendClients(clients map[string]storeapi.StoreApiClient, shards [][]string) error {
	for _, shard := range shards {
		for _, replica := range shard {
			if _, has := clients[replica]; has {
				continue
			}
			// this doesn't block, and if store is down, it will try to reconnect in background
			conn, err := grpc.NewClient(
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

func NewIngestor(iConfig IngestorConfig, store *storeapiclient.Store) (*Ingestor, error) {
	iConfig.setDefaults()

	rateLimiter := ratelimiter.NewRateLimiter(iConfig.API.QueryRateLimit, metric.RateLimiterSize.Set)

	ctx, cancel := context.WithCancel(context.Background())

	grpcGateway := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &humanReadableMarshaler{}),
	)
	err := seqproxyapi.RegisterSeqProxyApiHandlerFromEndpoint(ctx, grpcGateway, iConfig.API.GatewayAddr,
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
		iConfig.Bulk.HotStores = stores.NewStoresFromString("memory", 1)
		iConfig.Search.HotStores = stores.NewStoresFromString("memory", 1)
	} else {
		clients, err = clientsFromConfig(iConfig.Search)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("initialize clients: %s", err)
		}
	}

	searchIngestor := search.NewIngestor(iConfig.Search, clients)

	var bulkIngestor *bulk.Ingestor
	var bulkHandler http.Handler
	if config.UseRegions {
		bulkHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("bulk not supported in regions-only mode"))
		})
	} else {
		bulkClient := bulk.NewSeqDBClient(iConfig.Bulk.HotStores, iConfig.Bulk.WriteStores, iConfig.Bulk.BulkCircuit, clients)
		bulkIngestor = bulk.NewIngestor(iConfig.Bulk, bulkClient)
		bulkHandler = NewBulkHandler(bulkIngestor, iConfig.Bulk.MaxDocumentSize)
	}

	var mirror seqproxyapi.SeqProxyApiClient
	if iConfig.Search.MirrorAddr != "" {
		conn, err := grpc.NewClient(iConfig.Search.MirrorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Error("failed to create mirror client", zap.Error(err))
		} else {
			mirror = seqproxyapi.NewSeqProxyApiClient(conn)
		}
	}

	handler := newIngestorHandler(iConfig.API.EsVersion, bulkHandler, grpcGateway)

	return &Ingestor{
		Config:         iConfig,
		httpServer:     newHTTPServer(handler),
		grpcServer:     newGRPCServer(iConfig.API, searchIngestor, iConfig.Bulk.MappingProvider, rateLimiter, mirror),
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

	// nil in case of regions
	if i.BulkIngestor != nil {
		i.BulkIngestor.Stop()
	}
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

func (m humanReadableMarshaler) Unmarshal(data []byte, v interface{}) error {
	return m.stdlibMarshaler.Unmarshal(data, v)
}
