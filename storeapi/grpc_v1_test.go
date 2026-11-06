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
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/skipmaskmanager"
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

func TestBuildSearchResponseColumns(t *testing.T) {
	agg := seq.NewAggregatableSamplesForFunc(seq.AggFuncSum, 1, 2)
	agg.AppendRow([]string{"a"}, consts.DummyMID, seq.SamplesContainer{Total: 2, Sum: 4, NotExists: 7})
	agg.AppendRow([]string{"b"}, consts.DummyMID, seq.SamplesContainer{Total: 1, Sum: 1, NotExists: 8})
	agg.SortRows()

	t.Run("proxy protocol v2 compressed columns", func(t *testing.T) {
		resp := buildSearchResponseVersion(&seq.QPR{Aggs: []seq.AggregatableSamples{*agg}}, config.CurrentProxyProtocolVersion)
		values := resp.Aggs[0].GetValues()
		assert.NotNil(t, values)

		tokenColumn, err := seq.UnmarshalStringColumn(values.Tokens[0].Data)
		assert.NoError(t, err)
		requireRowsEqual(t, tokenColumn, "a", "b")
		assert.Empty(t, values.Mids)
		totals, err := seq.UnmarshalInt64Column(values.Totals)
		assert.NoError(t, err)
		var got []int64
		it := totals.NewIter()
		for it.Next() {
			got = append(got, it.Value())
		}
		assert.Equal(t, []int64{2, 1}, got)
		notExists, err := seq.UnmarshalInt64Column(values.NotExists)
		assert.NoError(t, err)
		got = nil
		it = notExists.NewIter()
		for it.Next() {
			got = append(got, it.Value())
		}
		assert.Equal(t, []int64{7, 8}, got)
		sums, err := seq.UnmarshalFloat64Column(values.Sums)
		assert.NoError(t, err)
		floatIt := sums.NewIter()
		gotFloat := make([]float64, 0)
		for floatIt.Next() {
			gotFloat = append(gotFloat, floatIt.Value())
		}
		assert.Equal(t, []float64{4, 1}, gotFloat)
	})

	t.Run("proxy protocol v2 compressed mids", func(t *testing.T) {
		timeSeries := seq.NewAggregatableSamplesForFunc(seq.AggFuncSum, 1, 3)
		timeSeries.AppendRow([]string{"a"}, 2, seq.SamplesContainer{Total: 2, Sum: 4})
		timeSeries.AppendRow([]string{"b"}, 2, seq.SamplesContainer{Total: 1, Sum: 1})
		timeSeries.AppendRow([]string{"a"}, 1, seq.SamplesContainer{Total: 3, Sum: 6})
		timeSeries.SortRows()

		resp := buildSearchResponseVersion(&seq.QPR{Aggs: []seq.AggregatableSamples{*timeSeries}}, config.CurrentProxyProtocolVersion)
		values := resp.Aggs[0].GetValues()
		mids, err := seq.UnmarshalInt64Column(values.Mids)
		assert.NoError(t, err)

		var got []int64
		it := mids.NewIter()
		for it.Next() {
			got = append(got, it.Value())
		}
		assert.Equal(t, []int64{2, 2, 1}, got)
	})
}

func requireRowsEqual(t *testing.T, col seq.StringColumn, want ...string) {
	t.Helper()
	var got []string
	it := col.NewIter()
	for it.Next() {
		got = append(got, string(it.Row()))
	}
	assert.Equal(t, want, got)
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

	skipMaskManager := skipmaskmanager.New(t.Context(), skipmaskmanager.Config{}, nil, mappingProvider)

	fm, stop, err := fracmanager.New(t.Context(), &fracmanager.Config{
		FracSize:  500,
		TotalSize: 5000,
		DataDir:   dataDir,
	}, nil, skipMaskManager)
	assert.NoError(t, err)

	apiConfig := APIConfig{
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

	g := NewGrpcV1(apiConfig, fm, mappingProvider)

	release := func() {
		stop()
		common.RemoveDir(dataDir)
	}

	return g, fm.WaitIdleForTests, release
}
