package setup

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"path/filepath"
	"runtime"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/alecthomas/units"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/network/circuitbreaker"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/proxyapi"
	"github.com/ozontech/seq-db/seq"
	seqs3 "github.com/ozontech/seq-db/storage/s3"
	"github.com/ozontech/seq-db/storeapi"
	"github.com/ozontech/seq-db/tests/common"
)

type TestingEnvConfig struct {
	Name              string
	DataDir           string
	IngestorCount     int
	ColdShards        int // number of replicaSets (aka shards)
	ColdFactor        int // number of replicas in each replicaSet
	HotShards         int // number of replicaSets (aka shards)
	HotFactor         int // number of replicas in each replicaSet
	HotModeEnabled    bool
	QueryRateLimit    *float64
	FracManagerConfig *fracmanager.Config

	Mapping        seq.Mapping
	IndexAllFields bool

	S3Cli *seqs3.Client
}

type Stores [][]*storeapi.Store

type TestingEnv struct {
	ingestorAccessCounter   *atomic.Uint64
	hotStoresAccessCounter  *atomic.Uint64
	coldStoresAccessCounter *atomic.Uint64

	Ingestors  []*Ingestor
	HotStores  Stores
	ColdStores Stores

	Config *TestingEnvConfig
}

func (cfg *TestingEnvConfig) GetColdFactor() int {
	if cfg.ColdFactor == 0 {
		return 1
	}
	return cfg.ColdFactor
}

func (cfg *TestingEnvConfig) GetHotFactor() int {
	if cfg.HotFactor == 0 {
		return 1
	}
	return cfg.HotFactor
}

func (cfg *TestingEnvConfig) GetFracManagerConfig(replicaID string) fracmanager.Config {
	c := cfg.FracManagerConfig
	if c == nil {
		// Fastest zstd compression, see: https://github.com/facebook/zstd/releases/tag/v1.3.4.
		const fastestZstdLevel = -5
		c = fracmanager.FillConfigWithDefault(&fracmanager.Config{
			FracSize:  256 * uint64(units.MiB),
			TotalSize: 1 * uint64(units.GiB),
			SealParams: frac.SealParams{
				IDsZstdLevel:           fastestZstdLevel,
				LIDsZstdLevel:          fastestZstdLevel,
				TokenListZstdLevel:     fastestZstdLevel,
				DocsPositionsZstdLevel: fastestZstdLevel,
				TokenTableZstdLevel:    fastestZstdLevel,
				DocBlocksZstdLevel:     fastestZstdLevel,
				DocBlockSize:           int(units.MiB) * 4,
			},
		})
	}
	c.DataDir = filepath.Join(cfg.DataDir, replicaID)
	return *c
}

func (cfg *TestingEnvConfig) GetStoreConfig(replicaID string, cold bool) storeapi.StoreConfig {
	mode := storeapi.StoreModeHot
	if cold {
		mode = storeapi.StoreModeCold
	}
	return storeapi.StoreConfig{
		FracManager: cfg.GetFracManagerConfig(replicaID),
		API: storeapi.APIConfig{
			StoreMode: mode,
			Search: storeapi.SearchConfig{
				WorkersCount:          128,
				FractionsPerIteration: runtime.GOMAXPROCS(0),
				RequestsLimit:         0,
				LogThreshold:          0,
			},
		},
	}
}

func createBucket() string {
	credp := credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint("http://localhost:9000/"),
		config.WithCredentialsProvider(credp),
		config.WithClientLogMode(aws.ClientLogMode(0)),
	)

	if err != nil {
		panic(fmt.Errorf("cannot load S3 config: %w", err))
	}

	s3cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	bucket := uuid.NewString()
	_, err = s3cli.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		panic(err)
	}

	return bucket
}

func NewTestingEnv(cfg *TestingEnvConfig) *TestingEnv {
	if cfg.IngestorCount < 1 || cfg.IngestorCount > 10 {
		logger.Fatal("wrong number of ingestors (1 --> 10)")
	}
	if cfg.ColdShards < 0 || cfg.ColdShards > 10 {
		logger.Fatal("wrong number of cold replicasets (0 --> 10)")
	}
	if cfg.ColdShards > 0 && (cfg.ColdFactor < 1 || cfg.ColdFactor > 10) {
		logger.Fatal("wrong number of cold replicas (1 --> 10)")
	}
	if cfg.HotShards < 1 || cfg.HotShards > 10 {
		logger.Fatal("wrong number of hot replicasets (1 --> 10)")
	}
	if cfg.HotFactor < 1 || cfg.HotFactor > 10 {
		logger.Fatal("wrong number of hot replicas (1 --> 10)")
	}
	if cfg.DataDir == "" {
		logger.Fatal("empty data dir")
	}

	if len(cfg.Mapping) == 0 && !cfg.IndexAllFields {
		cfg.Mapping = seq.TestMapping
	}

	if cfg.FracManagerConfig != nil && cfg.FracManagerConfig.OffloadingEnabled {
		cli, err := seqs3.NewClient(
			"http://localhost:9000/",
			"minioadmin", "minioadmin",
			"us-east-1", createBucket(), 0,
		)
		if err != nil {
			panic(err)
		}
		cfg.S3Cli = cli
	}

	hotStores, hotStoresList := MakeStores(cfg, cfg.GetHotFactor(), false)
	coldStores, coldStoresList := MakeStores(cfg, cfg.GetColdFactor(), true)

	ingestors := MakeIngestors(cfg, hotStoresList, coldStoresList)

	// TODO: for some reason waiting for service ready vars to be true is not enough, so general sleep still needed
	time.Sleep(500 * time.Millisecond)

	rand.Shuffle(len(ingestors), func(i, j int) { ingestors[i], ingestors[j] = ingestors[j], ingestors[i] })

	return &TestingEnv{
		Ingestors:  ingestors,
		HotStores:  hotStores,
		ColdStores: coldStores,

		ingestorAccessCounter:   new(atomic.Uint64),
		hotStoresAccessCounter:  new(atomic.Uint64),
		coldStoresAccessCounter: new(atomic.Uint64),

		Config: cfg,
	}
}

func flatten(storesList Stores) []*storeapi.Store {
	if len(storesList) == 0 {
		return nil
	}

	var list []*storeapi.Store
	for _, replicas := range storesList {
		list = append(list, replicas...)
	}

	rand.Shuffle(len(list), func(i, j int) { list[i], list[j] = list[j], list[i] })
	return list
}

func (cfg *TestingEnvConfig) GetHotStoresConfs() []storeapi.StoreConfig {
	cfgs := make([]storeapi.StoreConfig, 0)

	for i := 0; i < cfg.HotShards; i++ {
		for j := 0; j < cfg.HotFactor; j++ {
			replicaID := fmt.Sprintf("hot-%d-%d", i, j)
			cfgs = append(cfgs, cfg.GetStoreConfig(replicaID, false))
		}
	}

	return cfgs
}

func (cfg *TestingEnvConfig) GetColdStoresConfs() []storeapi.StoreConfig {
	cfgs := make([]storeapi.StoreConfig, 0)

	for i := 0; i < cfg.ColdShards; i++ {
		for j := 0; j < cfg.ColdFactor; j++ {
			replicaID := fmt.Sprintf("cold-%d-%d", i, j)
			cfgs = append(cfgs, cfg.GetStoreConfig(replicaID, true))
		}
	}

	return cfgs
}

func (cfg *TestingEnvConfig) MakeStores(
	confs []storeapi.StoreConfig, replicas int, s3cli *seqs3.Client,
) (Stores, [][]string) {
	replicaSets := len(confs) / replicas
	storesList := make(Stores, replicaSets)
	storesAddrs := make([][]string, replicaSets)

	for i := range confs {
		k := i / replicas
		common.CreateDir(confs[i].FracManager.DataDir)

		mappingProvider, err := mappingprovider.New(
			"",
			mappingprovider.WithMapping(cfg.Mapping),
			mappingprovider.WithIndexAllFields(cfg.IndexAllFields),
		)
		if err != nil {
			logger.Fatal("can't create mapping", zap.Error(err))
		}

		store, err := storeapi.NewStore(context.Background(), confs[i], s3cli, mappingProvider)
		if err != nil {
			panic(err)
		}

		lis := randomListener()
		store.Start(lis)

		storesList[k] = append(storesList[k], store)
		storesAddrs[k] = append(storesAddrs[k], store.GrpcAddr())
	}

	return storesList, storesAddrs
}

func MakeStores(cfg *TestingEnvConfig, replicas int, cold bool) (Stores, [][]string) {
	if cold {
		confs := cfg.GetColdStoresConfs()
		return cfg.MakeStores(confs, replicas, cfg.S3Cli)
	}

	confs := cfg.GetHotStoresConfs()
	return cfg.MakeStores(confs, replicas, cfg.S3Cli)
}

func newNetworkStores(ips [][]string) *stores.Stores {
	vers := make([]string, len(ips))
	for i := range ips {
		vers[i] = buildinfo.Version
	}
	return &stores.Stores{
		Shards: ips,
		Vers:   vers,
	}
}

type Ingestor struct {
	*proxyapi.Ingestor
	HTTPAddr string
}

func MakeIngestors(cfg *TestingEnvConfig, hot, cold [][]string) []*Ingestor {
	ingestors := make([]*Ingestor, cfg.IngestorCount)

	coldStores := newNetworkStores(cold)
	hotStores := newNetworkStores(hot)

	for i := range cfg.IngestorCount {
		httpLis := randomListener()
		grpcLis := randomListener()

		mappingProvider, err := mappingprovider.New(
			"",
			mappingprovider.WithMapping(cfg.Mapping),
			mappingprovider.WithIndexAllFields(cfg.IndexAllFields),
		)
		if err != nil {
			logger.Fatal("can't create mapping", zap.Error(err))
		}

		proxyIngestor, err := proxyapi.NewIngestor(
			proxyapi.IngestorConfig{
				API: proxyapi.APIConfig{
					SearchTimeout:  10 * time.Minute, // long enough for debugging purposes with a debugger
					ExportTimeout:  10 * time.Minute, // the same (debugging purposes)
					QueryRateLimit: 0,
					EsVersion:      "test",
					GatewayAddr:    grpcLis.Addr().String(),
				},
				Bulk: bulk.IngestorConfig{
					HotStores:   hotStores,
					WriteStores: coldStores,
					BulkCircuit: circuitbreaker.Config{
						RequestVolumeThreshold: 101, // disable circuit breaker
						Timeout:                time.Hour,
					},
					MaxInflightBulks:       0,
					AllowedTimeDrift:       24 * time.Hour,
					FutureAllowedTimeDrift: 24 * time.Hour,
					MappingProvider:        mappingProvider,
					MaxTokenSize:           consts.DefaultMaxTokenSize,
					CaseSensitive:          false,
					PartialFieldIndexing:   false,
					DocsZSTDCompressLevel:  -1,
					MetasZSTDCompressLevel: -1,
					MaxDocumentSize:        int(units.MiB + units.KiB),
				},
				Search: search.Config{
					HotStores:       hotStores,
					HotReadStores:   nil,
					ReadStores:      coldStores,
					WriteStores:     coldStores,
					ShuffleReplicas: false,
				},
			},
			nil,
		)
		if err != nil {
			logger.Fatal("error during ingestor init", zap.Error(err))
		}

		ingestors[i] = &Ingestor{
			Ingestor: proxyIngestor,
			HTTPAddr: httpLis.Addr().String(),
		}

		ingestors[i].Start(httpLis, grpcLis)
	}

	return ingestors
}

// Ingestor returns "random" ingestor managed by TestingEnv
// but guarantees that each store will return at least once
func (t *TestingEnv) Ingestor() *Ingestor {
	i := int(t.ingestorAccessCounter.Inc()) % len(t.Ingestors)
	return t.Ingestors[i]
}

// Store returns random store managed by TestingEnv
// but guarantees that each store will return at least once
func (t *TestingEnv) Store(hot bool) *storeapi.Store {
	allStores := flatten(t.ColdStores)
	counter := t.coldStoresAccessCounter

	if hot {
		allStores = flatten(t.HotStores)
		counter = t.hotStoresAccessCounter
	}

	return allStores[int(counter.Inc())%len(allStores)]
}

func (t *TestingEnv) IngestorAddr() string {
	return "http://" + t.Ingestor().HTTPAddr
}

// IngestorBulkAddr returns "random" ingestor HTTP address
// but guarantees that each store will return at least once
func (t *TestingEnv) IngestorBulkAddr() string {
	return t.IngestorAddr() + "/_bulk"
}

// IngestorSearchAddr returns "random" ingestor HTTP address
// but guarantees that each store will return at least once
func (t *TestingEnv) IngestorSearchAddr() string {
	return t.IngestorAddr() + "/search"
}

func (t *TestingEnv) IngestorFetchAddr() string {
	return t.IngestorAddr() + "/fetch"
}

func randomListener() (lis net.Listener) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:0", common.Localhost))
	if err != nil {
		panic(err)
	}
	return lis
}

type storeCallback func(*storeapi.Store)

func (s Stores) apply(c storeCallback) {
	for _, replicaSet := range s {
		for _, replica := range replicaSet {
			c(replica)
		}
	}
}

func (s Stores) WaitIdle() {
	s.apply(func(s *storeapi.Store) {
		s.WaitIdle()
	})
}

func (s Stores) Stop() {
	s.apply(func(s *storeapi.Store) {
		s.Stop()
	})
}

func (s Stores) SealAll() {
	s.apply(func(s *storeapi.Store) {
		s.SealAll()
	})
}

func (s Stores) OffloadAll() {
	s.apply(func(s *storeapi.Store) {
		s.OffloadAll()
	})
}

func (s Stores) ResetCache() {
	s.apply(func(s *storeapi.Store) {
		s.ResetCache()
	})
}

func (s Stores) CountInstances() int {
	sum := 0
	for _, replicaSet := range s {
		sum += len(replicaSet)
	}
	return sum
}

func (t *TestingEnv) SealAll() {
	t.HotStores.SealAll()
	t.ColdStores.SealAll()
}

func (t *TestingEnv) OffloadAll() {
	t.HotStores.OffloadAll()
	t.ColdStores.OffloadAll()
}

func (t *TestingEnv) WaitIdle() {
	t.HotStores.WaitIdle()
	t.ColdStores.WaitIdle()
}

func (t *TestingEnv) ResetCache() {
	t.HotStores.ResetCache()
	t.ColdStores.ResetCache()
}

func (t *TestingEnv) StopStore() {
	t.HotStores.Stop()
	t.ColdStores.Stop()
}

func (t *TestingEnv) StopIngestor() {
	for _, ing := range t.Ingestors {
		ing.Stop()
	}
}

func (t *TestingEnv) StopAll() {
	t.StopIngestor()
	t.StopStore()
}

type SearchOption func(sr *search.SearchRequest)

func NoFetch() SearchOption {
	return func(sr *search.SearchRequest) {
		sr.ShouldFetch = false
	}
}

func WithTotal(f bool) SearchOption {
	return func(sr *search.SearchRequest) {
		sr.WithTotal = f
	}
}

func WithOffset(offset int) SearchOption {
	return func(sr *search.SearchRequest) {
		sr.Offset = offset
	}
}

// WithAggQuery adds aggregation query to search request. Aggregations parameters are
// passed as consequent strings of aggregation fields and filters:
// aggField1, aggFilter1, aggField2, aggFilter2, ..., aggFieldN, aggFilterN.
//
// For example, if two aggregations without filters are needed, this function should be
// called with args: "agg1", "", "agg2".
//
// If called with single empty string, no aggregation query is added.
func WithAggQuery(aggQueries ...any) SearchOption {
	aggs := make([]search.AggQuery, 0, len(aggQueries))
	for _, aggQuery := range aggQueries {
		switch aggQuery := aggQuery.(type) {
		case string:
			aggs = append(aggs, search.AggQuery{Field: aggQuery})
		case search.AggQuery:
			aggs = append(aggs, aggQuery)
		default:
			panic("unknown query type")
		}
	}
	return func(sr *search.SearchRequest) {
		sr.AggQ = append(sr.AggQ, aggs...)
	}
}

func WithInterval(interval time.Duration) SearchOption {
	return func(sr *search.SearchRequest) {
		sr.Interval = seq.MID(interval / time.Millisecond)
	}
}

func WithTimeRange(from, to time.Time) SearchOption {
	return func(sr *search.SearchRequest) {
		sr.From = seq.MID(from.UnixMilli())
		sr.To = seq.MID(to.UnixMilli())
	}
}

func WithOrder(o seq.DocsOrder) SearchOption {
	return func(sr *search.SearchRequest) {
		sr.Order = o
	}
}

func (t *TestingEnv) Search(q string, size int, options ...SearchOption) (*seq.QPR, [][]byte, time.Duration, error) {
	sr := &search.SearchRequest{
		Explain:     false,
		Q:           []byte(q),
		Offset:      0,
		Size:        size,
		From:        0,
		To:          math.MaxUint64,
		WithTotal:   true,
		ShouldFetch: true,
		Order:       seq.DocsOrderDesc,
	}

	for _, option := range options {
		option(sr)
	}

	var docs [][]byte
	qpr, docsStream, duration, err := t.Ingestor().SearchIngestor.Search(context.Background(), sr, nil)
	if docsStream != nil {
		docs = search.ReadAll(docsStream)
	}
	return qpr, docs, duration, err
}

func (t *TestingEnv) Fetch(ids []seq.ID) ([][]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := t.Ingestor().SearchIngestor.Documents(ctx, search.FetchRequest{IDs: ids})
	if err != nil {
		return nil, err
	}
	return search.ReadAll(stream), nil
}
