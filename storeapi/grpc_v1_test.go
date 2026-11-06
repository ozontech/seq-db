package storeapi

import (
	"context"
	"math"
	"path"
	"strconv"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

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

func TestProxyProtocolFromContext(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		expect config.ProxyProtocolVersion
	}{
		{name: "no metadata", ctx: context.Background(), expect: config.ProxyProtocolVersion1},
		{
			name:   "no protocol header",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.MD{}),
			expect: config.ProxyProtocolVersion1,
		},
		{
			name:   "protocol v1",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.Pairs(consts.ProxyProtocolVersionHeader, "1")),
			expect: config.ProxyProtocolVersion1,
		},
		{
			name:   "protocol v2",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.Pairs(consts.ProxyProtocolVersionHeader, "2")),
			expect: config.ProxyProtocolVersion2,
		},
		{
			name:   "future protocol",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.Pairs(consts.ProxyProtocolVersionHeader, "3")),
			expect: config.ProxyProtocolVersion2,
		},
		{
			name:   "unknown protocol",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.Pairs(consts.ProxyProtocolVersionHeader, "unknown")),
			expect: config.ProxyProtocolVersion2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, proxyProtocolFromContext(tt.ctx))
		})
	}
}

func TestBuildSearchResponseColumns(t *testing.T) {
	agg := seq.NewAggregatableSamples(seq.AggFuncSum, 1, 2)
	agg.Tokens[0].AppendString("a")
	agg.Values.Totals.Append(2)
	agg.Values.NotExists.Append(7)
	agg.Values.Sums.Append(4)
	agg.Values.Mins.Append(0)
	agg.Values.Maxs.Append(0)
	agg.Tokens[0].AppendString("b")
	agg.Values.Totals.Append(1)
	agg.Values.NotExists.Append(8)
	agg.Values.Sums.Append(1)
	agg.Values.Mins.Append(0)
	agg.Values.Maxs.Append(0)

	t.Run("proxy protocol v2 columnar response", func(t *testing.T) {
		resp := buildSearchResponseVersion(&seq.QPR{Aggs: []seq.AggregatableSamples{*agg}}, config.CurrentProxyProtocolVersion)
		values := resp.Aggs[0].GetValues()
		assert.NotNil(t, values)

		tokenColumn, err := seq.UnmarshalStringColumn(values.Tokens[0])
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

	t.Run("proxy protocol v2 columnar mids", func(t *testing.T) {
		timeSeries := seq.NewAggregatableSamples(seq.AggFuncSum, 1, 3)
		timeSeries.MIDs = seq.NewInt64Column(3)
		appendSumRow := func(token string, mid int64, total int64, sum float64) {
			timeSeries.Tokens[0].AppendString(token)
			timeSeries.MIDs.Append(mid)
			timeSeries.Values.Totals.Append(total)
			timeSeries.Values.NotExists.Append(0)
			timeSeries.Values.Sums.Append(sum)
			timeSeries.Values.Mins.Append(0)
			timeSeries.Values.Maxs.Append(0)
		}
		appendSumRow("a", 2, 2, 4)
		appendSumRow("b", 2, 1, 1)
		appendSumRow("a", 1, 3, 6)

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

	t.Run("legacy zero-row aggregation", func(t *testing.T) {
		qpr := &seq.QPR{
			Aggs: make([]seq.AggregatableSamples, 1),
		}

		resp := buildSearchResponseVersion(qpr, config.ProxyProtocolVersion1)

		require.Len(t, resp.Aggs, 1)
		require.Empty(t, resp.Aggs[0].Timeseries)
		require.Empty(t, resp.Aggs[0].Agg)
		require.Empty(t, resp.Aggs[0].AggHistogram)
	})
}

func TestAggValuesToProtoAllColumns(t *testing.T) {
	funcs := []seq.AggFunc{
		seq.AggFuncCount,
		seq.AggFuncSum,
		seq.AggFuncMin,
		seq.AggFuncMax,
		seq.AggFuncAvg,
		seq.AggFuncQuantile,
		seq.AggFuncUnique,
		seq.AggFuncUniqueCount,
	}

	for _, fn := range funcs {
		t.Run(fn.String(), func(t *testing.T) {
			agg := seq.NewAggregatableSamples(fn, 1, 1)
			agg.AppendSamples([]string{"a"}, consts.DummyMID, seq.SamplesContainer{
				Total:     1,
				NotExists: 2,
				Sum:       3,
				Min:       4,
				Max:       5,
				Samples:   []float64{6},
				Values:    map[uint32]struct{}{7: {}},
			})

			encoded := aggValuesToProto(agg)
			assert.NotEmpty(t, encoded.Totals)
			assert.NotEmpty(t, encoded.NotExists)
			assert.NotEmpty(t, encoded.Sums)
			assert.NotEmpty(t, encoded.Mins)
			assert.NotEmpty(t, encoded.Maxs)
			assert.NotEmpty(t, encoded.Quantiles)
			assert.NotEmpty(t, encoded.UniqueIds)
		})
	}
}

func requireRowsEqual(t *testing.T, col seq.StringColumn, want ...string) {
	t.Helper()
	var got []string
	it := col.NewIter()
	for it.Next() {
		got = append(got, string(it.Value()))
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
