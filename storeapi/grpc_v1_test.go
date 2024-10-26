package storeapi

import (
	"context"
	"math"
	"strconv"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
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

	dp := frac.NewDocProvider()
	for i := 0; i < cnt; i++ {
		id := seq.SimpleID(i + 1)
		doc := []byte("document")
		tokens := seq.Tokens("_all_:", "service:100500", "k8s_pod:"+strconv.Itoa(i))
		dp.Append(doc, nil, id, tokens)
	}
	req := &storeapi.BulkRequest{Count: int64(cnt)}
	req.Docs, req.Metas = dp.Provide()
	return req
}

func getTestGrpc(t *testing.T) (*GrpcV1, func(), func()) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)

	fm := fracmanager.NewFracManager(&fracmanager.Config{
		FracSize:         500,
		TotalSize:        5000,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})
	assert.NoError(t, fm.Load(context.Background()))
	fm.Start()

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
		},
		Fetch: FetchConfig{
			LogThreshold: 0,
		},
	}

	g := NewGrpcV1(config, fm, seq.TestMapping)

	release := func() {
		fm.Stop()
		common.RemoveDir(dataDir)
	}

	return g, fm.WaitIdle, release
}
