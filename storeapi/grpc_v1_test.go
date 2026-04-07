package storeapi

import (
	"context"
	"math"
	"path"
	"strconv"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/asyncsearcher"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/filtermanager"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

func TestDuplicates(t *testing.T) {
	s, waitIdle, release := getTestGrpc(t)
	defer release()

	cnt := 15
	ctx := context.Background()

	// add bulk
	_, err := s.Bulk(ctx, makeBulkRequest(cnt-5))
	assert.NoError(t, err)

	// add doubles
	_, err = s.Bulk(ctx, makeBulkRequest(cnt))
	assert.NoError(t, err)

	waitIdle()

	searchReq := &storeapi.SearchRequest{
		Query: "service:100500",
		From:  0,
		To:    math.MaxInt64,
		Size:  100,
	}
	data, err := s.Search(ctx, searchReq)
	assert.NoError(t, err)
	assert.Equal(t, cnt, len(data.IdSources), "we expect no doubles")
}

func makeBulkRequest(cnt int) *storeapi.BulkRequest {
	metaRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(metaRoot)

	dp := indexer.NewTestDocProvider()
	for i := 0; i < cnt; i++ {
		id := seq.SimpleID(int64(i + 1))
		doc := []byte("document")
		dp.Append(doc, id, "_all_:", "service:100500", "k8s_pod:"+strconv.Itoa(i))
	}
	req := &storeapi.BulkRequest{Count: int64(cnt)}
	req.Docs, req.Metas = dp.Provide()
	return req
}

func getTestGrpc(t *testing.T) (*GrpcV1, func(), func()) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)

	mappingProvider, err := mappingprovider.New("", mappingprovider.WithMapping(seq.TestMapping))
	assert.NoError(t, err)

	filterManager := filtermanager.New(t.Context(), filtermanager.Config{}, nil, mappingProvider)

	fm, stop, err := fracmanager.New(t.Context(), &fracmanager.Config{
		FracSize:  500,
		TotalSize: 5000,
		DataDir:   dataDir,
	}, nil, filterManager)
	assert.NoError(t, err)

	config := APIConfig{
		StoreMode: "",
		Bulk: BulkConfig{
			RequestsLimit: consts.DefaultBulkRequestsLimit,
			LogThreshold:  0,
		},
		Search: SearchConfig{
			WorkersCount:          1,
			FractionsPerIteration: 1,
			RequestsLimit:         consts.DefaultSearchRequestsLimit,
			LogThreshold:          0,
			Async:                 asyncsearcher.AsyncSearcherConfig{DataDir: path.Join(dataDir, "async_search")},
		},
		Fetch: FetchConfig{
			LogThreshold: 0,
		},
	}

	g := NewGrpcV1(config, fm, mappingProvider)

	release := func() {
		stop()
		common.RemoveDir(dataDir)
	}

	return g, fm.WaitIdleForTests, release
}
