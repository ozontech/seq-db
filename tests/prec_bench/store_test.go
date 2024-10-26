package prec_bench

import (
	"math"
	"math/rand"
	"runtime"
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	api "github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storeapi"
	"github.com/ozontech/seq-db/tests/prec_bench/bare_store"
	"github.com/ozontech/seq-db/tests/prec_bench/bench"
)

func BenchmarkEmptyStoreSearch(b *testing.B) {
	storeConfig := ActiveStoreConfig()
	MakeTempDir(b, storeConfig)

	payload, meter, cleaner := SetupStoreSearchBench(
		storeConfig,
		&bare_store.BareStorePayloadConfig{
			SearchParallel: 64,
		},
	)
	payload.SearchRequests = FloodSearchAll(cleaner)

	bench.Run(b, payload, meter, cleaner)
}

func BenchmarkTheBenchItself(b *testing.B) {
	cleaner := &bench.Cleaner{}
	meter := bench.NewWarmupMeter(
		&bench.IncMeter{},
		int32(1),
		1*time.Nanosecond,
	)
	payload := bare_store.NewBench(
		&bare_store.BareStorePayloadConfig{
			SearchParallel: 64,
		},
		&bare_store.MockStore{
			Sleep: 1 * time.Microsecond,
		},
		nil,
		meter,
	)
	payload.SearchRequests = FloodSearchAll(cleaner)

	bench.Run(b, payload, meter, cleaner)
}

func BenchmarkRWContention(b *testing.B) {
	var storeConfig = ActiveStoreConfig()
	MakeTempDir(b, storeConfig)

	payload, meter, cleaner := SetupStoreSearchBench(
		storeConfig,
		&bare_store.BareStorePayloadConfig{
			BulkParallel:    10,
			SearchParallel:  10,
			PreloadParallel: 10,
		},
	)

	data := [][]byte{
		[]byte("{\"service\":\"BigA\", \"message\":\"New day here\"}"),
		[]byte("{\"service\":\"BigB\", \"message\":\"Good bye\"}"),
		[]byte("{\"service\":\"BigC\", \"message\":\"Good day\"}"),
	}
	payload.PreloadBulkRequests = bench.FloodFixedAmount(10, SimpleBulkGenerator(cleaner, 1000, time.Millisecond, data))

	payload.BulkRequests = bench.Flood(cleaner, SimpleBulkGenerator(cleaner, 1000, time.Millisecond, data))

	payload.SearchRequests =
		bench.Flood(cleaner, bench.ConstSource(&api.SearchRequest{
			Query:     "not service:abc",
			To:        math.MaxInt64,
			WithTotal: true,
		}))

	bench.Run(b, payload, meter, cleaner)
}

func BenchmarkActiveStoreSearchA(b *testing.B) {
	var storeConfig = ActiveStoreConfig()
	MakeTempDir(b, storeConfig)

	procs := runtime.GOMAXPROCS(0)

	payload, meter, cleaner := SetupStoreSearchBench(
		storeConfig,
		&bare_store.BareStorePayloadConfig{
			SearchParallel: procs,
		},
	)
	payload.PreloadBulkRequests = bench.FloodFixedAmount(
		1000,
		SimpleLoad(cleaner, 250, 0.99),
	)
	payload.SearchRequests =
		bench.Flood(cleaner, bench.ConstSource(&api.SearchRequest{
			Query: "service:Small",
			To:    math.MaxInt64,
			Size:  10,
		}))

	bench.Run(b, payload, meter, cleaner)
}

func BenchmarkActiveStoreSearchB(b *testing.B) {
	var storeConfig = ActiveStoreConfig()
	MakeTempDir(b, storeConfig)

	procs := runtime.GOMAXPROCS(0)

	payload, meter, cleaner := SetupStoreSearchBench(
		storeConfig,
		&bare_store.BareStorePayloadConfig{
			SearchParallel: procs,
		},
	)
	payload.PreloadBulkRequests = bench.FloodFixedAmount(
		1000,
		SimpleLoad(cleaner, 25, 0.5),
	)
	payload.SearchRequests =
		bench.Flood(cleaner, bench.ConstSource(&api.SearchRequest{
			Query: "service:Small",
			To:    math.MaxInt64,
			Size:  10,
		}))

	bench.Run(b, payload, meter, cleaner)
}

func BenchmarkSealedStoreSearchA(b *testing.B) {
	var storeConfig = SealedStoreConfig(10 * consts.KB)
	MakeTempDir(b, storeConfig)

	procs := runtime.GOMAXPROCS(0)

	payload, meter, cleaner := SetupStoreSearchBench(
		storeConfig,
		&bare_store.BareStorePayloadConfig{
			SearchParallel: procs,
		},
	)
	payload.PreloadBulkRequests = bench.FloodFixedAmount(
		1000,
		SimpleLoad(cleaner, 2500, 0.99),
	)
	payload.SearchRequests =
		bench.Flood(cleaner, bench.ConstSource(&api.SearchRequest{
			Query: "service:BigB AND message:day",
			To:    math.MaxInt64,
		}))

	bench.Run(b, payload, meter, cleaner)
}

func BenchmarkSealedStoreSearchB(b *testing.B) {
	var storeConfig = SealedStoreConfig(10 * consts.KB)
	MakeTempDir(b, storeConfig)

	procs := runtime.GOMAXPROCS(0)

	payload, meter, cleaner := SetupStoreSearchBench(
		storeConfig,
		&bare_store.BareStorePayloadConfig{
			SearchParallel: procs,
		},
	)
	payload.PreloadBulkRequests = bench.FloodFixedAmount(
		1000,
		SimpleLoad(cleaner, 25, 0.8),
	)
	payload.SearchRequests =
		bench.Flood(cleaner, bench.ConstSource(&api.SearchRequest{
			Query: "service:BigB AND message:day",
			To:    math.MaxInt64,
		}))

	bench.Run(b, payload, meter, cleaner)
}

func SimpleLoad(cleaner *bench.Cleaner, bigLength int, bigShare float32) func() *api.BulkRequest {
	return bench.MixSources(bigShare,
		SimpleBulkGenerator(
			cleaner,
			bigLength,
			10*time.Millisecond,
			[][]byte{
				[]byte("{\"service\":\"BigA\"}"),
				[]byte("{\"service\":\"BigB\", \"message\":\"New day here\"}"),
				[]byte("{\"service\":\"BigB\", \"message\":\"Good bye\"}"),
				[]byte("{\"service\":\"BigC\", \"message\":\"Good day\"}"),
			},
		), SimpleBulkGenerator(
			cleaner,
			10,
			100*time.Millisecond,
			[][]byte{
				[]byte("{\"service\":\"Small\", \"message\":\"Nobody's here\"}"),
				[]byte("{\"service\":\"Small\", \"message\":\"Some wise quote about day to day living\"}"),
			},
		),
	)
}

// doesn't put timestamp to the document
func SimpleBulkGenerator(cleaner *bench.Cleaner, size int, maxLag time.Duration, docs [][]byte) func() *api.BulkRequest {
	dp := frac.NewDocProvider()
	docRoots := make([]*insaneJSON.Root, len(docs))
	for i, d := range docs {
		docRoots[i] = insaneJSON.Spawn()
		err := docRoots[i].DecodeBytes(d)
		if err != nil {
			panic("Incorrect document")
		}
	}
	cleaner.CleanUp(func() {
		for _, r := range docRoots {
			insaneJSON.Release(r)
		}
	})
	return func() *api.BulkRequest {
		defer dp.TryReset()
		for i := 0; i < size; i++ {
			for idx, d := range docs {
				now := time.Now().Add(-time.Duration(rand.Int63n(int64(maxLag))))
				id := seq.NewID(now, rand.Uint64())
				dp.Append(d, docRoots[idx], id, nil)
			}
		}
		docsBlock, metasBlock := dp.Provide()
		return &api.BulkRequest{
			Count: int64(dp.DocCount),
			Docs:  docsBlock,
			Metas: metasBlock,
		}
	}
}

func FloodSearchAll(cleaner *bench.Cleaner) <-chan *api.SearchRequest {
	return bench.Flood(cleaner, bench.ConstSource(&api.SearchRequest{
		To: math.MaxInt64,
	}))
}

func SetupStoreSearchBench(
	storeConfig *storeapi.StoreConfig,
	payloadConfig *bare_store.BareStorePayloadConfig,
) (*bare_store.BareStorePayload, bench.Meter, *bench.Cleaner) {
	cleaner := &bench.Cleaner{}
	// values here are big enough to work, small enough to be unnoticeable
	meter := bench.NewWarmupMeter(
		&bench.IncMeter{},
		int32(payloadConfig.SearchParallel*4),
		100*time.Millisecond,
	)
	store := bare_store.NewBareStore(storeConfig, seq.TestMapping)
	storeBench := bare_store.NewBench(payloadConfig, store, nil, meter)
	return storeBench, meter, cleaner
}

func MakeTempDir(b *testing.B, config *storeapi.StoreConfig) {
	if config.FracManager.DataDir == "" {
		config.FracManager.DataDir = b.TempDir()
	}
}

func StoreConfigBase() *storeapi.StoreConfig {
	return &storeapi.StoreConfig{
		FracManager: *fracmanager.FillConfigWithDefault(&fracmanager.Config{
			FracLoadLimit:    0,
			ShouldRemoveMeta: true,
			MaxFractionHits:  4096,
		}),
		API: storeapi.APIConfig{
			StoreMode: "",
			Bulk: storeapi.BulkConfig{
				RequestsLimit: 16,
				LogThreshold:  0,
			},
			Search: storeapi.SearchConfig{
				WorkersCount:          128,
				FractionsPerIteration: runtime.GOMAXPROCS(0),
				RequestsLimit:         30,
				LogThreshold:          time.Second * 3,
			},
			Fetch: storeapi.FetchConfig{
				LogThreshold: time.Second * 3,
			},
		},
	}
}

func RemoveLimits(config *storeapi.StoreConfig) {
	config.API.Bulk.RequestsLimit = 1600
	config.API.Search.RequestsLimit = 3000
}

func EnableActiveOnly(config *storeapi.StoreConfig) {
	config.FracManager.FracSize = 10 * consts.GB
	config.FracManager.TotalSize = 10 * consts.GB
	config.FracManager.CacheSize = consts.GB
	config.FracManager.MaintenanceDelay = 24 * time.Hour
}

func EnableSealed(fracSize uint64, config *storeapi.StoreConfig) {
	config.FracManager.FracSize = fracSize
	config.FracManager.TotalSize = 10 * consts.GB
	config.FracManager.CacheSize = consts.GB
	config.FracManager.MaintenanceDelay = 1 * time.Millisecond
}

// intended for benchmarking search over active fraction
func ActiveStoreConfig() *storeapi.StoreConfig {
	config := StoreConfigBase()
	RemoveLimits(config)
	EnableActiveOnly(config)
	return config
}

// intended for benchmarking search over sealed fractions
func SealedStoreConfig(fracSize uint64) *storeapi.StoreConfig {
	config := StoreConfigBase()
	RemoveLimits(config)
	EnableSealed(fracSize, config)
	return config
}
