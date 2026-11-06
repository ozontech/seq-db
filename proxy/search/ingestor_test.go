package search

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search/mock"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func TestAggMin(t *testing.T) {
	searchIngestor := NewIngestor(Config{}, map[string]storeapi.StoreApiClient{})
	now, _ := time.Parse(time.RFC3339, "2050-01-01T10:00:00.000Z")

	ids := []seq.ID{
		seq.NewID(now.Add(time.Minute*15), 0),
		seq.NewID(now.Add(time.Minute*14), 0),
		seq.NewID(now.Add(time.Minute*10), 1),
		seq.NewID(now.Add(time.Minute*10), 0),
		seq.NewID(now.Add(time.Minute*5), 0),
		seq.NewID(now.Add(time.Minute*0), 0),
	}

	times, docs, _ := searchIngestor.aggregate(nil, nil, seq.TimeToMID(now.Add(time.Minute*15)), seq.DurationToMID(time.Minute*5), ids)
	assert.Equal(t, 4, len(docs), "wrong bucket count")
	assert.Equal(t, 3, docs[1], "wrong doc count")
	assert.Equal(t, 4, len(times), "wrong bucket count")
}

func TestAggHour(t *testing.T) {
	searchIngestor := NewIngestor(Config{}, map[string]storeapi.StoreApiClient{})
	now, _ := time.Parse(time.RFC3339, "2050-01-01T10:00:00.000Z")

	ids := []seq.ID{
		seq.NewID(now.Add(time.Minute+2), 0),
		seq.NewID(now.Add(time.Minute+1), 0),
		seq.NewID(now.Add(time.Minute-1), 0),
		seq.NewID(now.Add(time.Minute-2), 0),
	}

	times, docs, _ := searchIngestor.aggregate(nil, nil, seq.TimeToMID(now.Add(time.Hour)), seq.DurationToMID(time.Hour), ids)
	assert.Equal(t, 2, len(docs), "wrong bucket count")
	assert.Equal(t, 2, len(times), "wrong bucket count")
}

func TestAggValuesToQPR(t *testing.T) {
	values := []string{"a", "b"}

	t.Run("compressed columns", func(t *testing.T) {
		tokens := seq.NewStringColumn(len(values))
		for _, v := range values {
			tokens.AppendString(v)
		}
		totals := seq.NewInt64Column(len(values))
		totals.Append(2)
		totals.Append(1)
		notExists := seq.NewInt64Column(len(values))
		notExists.Append(7)
		notExists.Append(8)
		sums := seq.NewFloat64Column(len(values))
		sums.Append(4)
		sums.Append(1)
		mins := seq.NewFloat64Column(len(values))
		mins.Append(1)
		mins.Append(1)
		maxs := seq.NewFloat64Column(len(values))
		maxs.Append(3)
		maxs.Append(1)
		encoded := &storeapi.SearchResponse_AggValues{
			Func:      storeapi.AggFunc_AGG_FUNC_SUM,
			Tokens:    []*storeapi.StringColumn{{Data: tokens.AppendTo(nil)}},
			Totals:    totals.AppendTo(nil),
			NotExists: notExists.AppendTo(nil),
			Sums:      sums.AppendTo(nil),
			Mins:      mins.AppendTo(nil),
			Maxs:      maxs.AppendTo(nil),
			Quantiles: seq.ArraysColumn[float64]{}.AppendTo(nil),
			UniqueIds: seq.ArraysColumn[uint32]{}.AppendTo(nil),
		}

		agg, err := aggValuesToQPR(encoded)
		assert.NoError(t, err)
		requireAggTokens(t, &agg, values...)
	})

	t.Run("compressed mids", func(t *testing.T) {
		tokens := seq.NewStringColumn(len(values))
		for _, v := range values {
			tokens.AppendString(v)
		}
		mids := seq.NewInt64Column(len(values))
		mids.Append(2)
		mids.Append(1)
		totals := seq.NewInt64Column(len(values))
		totals.Append(2)
		totals.Append(1)
		encoded := &storeapi.SearchResponse_AggValues{
			Func:      storeapi.AggFunc_AGG_FUNC_COUNT,
			Tokens:    []*storeapi.StringColumn{{Data: tokens.AppendTo(nil)}},
			Mids:      mids.AppendTo(nil),
			Totals:    totals.AppendTo(nil),
			NotExists: seq.NewInt64Column(0).AppendTo(nil),
			Sums:      seq.NewFloat64Column(0).AppendTo(nil),
			Mins:      seq.NewFloat64Column(0).AppendTo(nil),
			Maxs:      seq.NewFloat64Column(0).AppendTo(nil),
			Quantiles: seq.ArraysColumn[float64]{}.AppendTo(nil),
			UniqueIds: seq.ArraysColumn[uint32]{}.AppendTo(nil),
		}

		agg, err := aggValuesToQPR(encoded)
		assert.NoError(t, err)
		var got []seq.MID
		it := agg.MIDs.NewIter()
		for it.Next() {
			got = append(got, seq.MID(it.Value()))
		}
		assert.Equal(t, []seq.MID{2, 1}, got)
	})

	t.Run("invalid token column is rejected", func(t *testing.T) {
		encoded := &storeapi.SearchResponse_AggValues{
			Func:   storeapi.AggFunc_AGG_FUNC_SUM,
			Tokens: []*storeapi.StringColumn{{Data: []byte("ab")}},
		}
		_, err := aggValuesToQPR(encoded)
		assert.ErrorContains(t, err, "not enough data")
	})

	t.Run("mismatched mids are rejected", func(t *testing.T) {
		tokens := seq.NewStringColumn(len(values))
		for _, v := range values {
			tokens.AppendString(v)
		}
		mids := seq.NewInt64Column(1)
		mids.Append(1)
		encoded := &storeapi.SearchResponse_AggValues{
			Func:   storeapi.AggFunc_AGG_FUNC_SUM,
			Tokens: []*storeapi.StringColumn{{Data: tokens.AppendTo(nil)}},
			Mids:   mids.AppendTo(nil),
		}

		_, err := aggValuesToQPR(encoded)
		assert.ErrorContains(t, err, "invalid mids column length")
	})
}

func TestResponseToQPRLegacyAggregation(t *testing.T) {
	now := time.Unix(1000000, 0)
	resp := &storeapi.SearchResponse{
		Aggs: []*storeapi.SearchResponse_Agg{{
			Timeseries: []*storeapi.SearchResponse_Bin{
				{
					Label: "b",
					Ts:    timestamppb.New(now),
					Hist: &storeapi.SearchResponse_Histogram{
						Total:  2,
						Values: []uint32{3, 3},
					},
				},
				{
					Label: "a",
					Ts:    timestamppb.New(now),
					Hist: &storeapi.SearchResponse_Histogram{
						Total:  1,
						Values: []uint32{3},
					},
				},
				{
					Label: "z",
					Ts:    timestamppb.New(now.Add(-time.Minute)),
					Hist: &storeapi.SearchResponse_Histogram{
						Total: 4,
					},
				},
			},
			NotExists: 7,
			ValuesPool: []string{
				"value",
			},
		}},
	}

	qpr := responseToQPR(resp, 11, false)
	require.Len(t, qpr.Aggs, 1)

	agg := qpr.Aggs[0]
	require.Equal(t, seq.AggFunc(seq.AggFuncUniqueCount), agg.Values.Func)
	require.Equal(t, int64(7), agg.NotExists)
	require.Equal(t, []string{"value"}, agg.ValuesPool)

	var tokens []string
	it := agg.Tokens[0].NewIter()
	for it.Next() {
		tokens = append(tokens, string(it.Row()))
	}
	require.Equal(t, []string{"a", "b", "z"}, tokens)
	require.Equal(t, seq.MID(now.UnixNano()), agg.RowMID(0))
	require.Equal(t, seq.MID(now.Add(-time.Minute).UnixNano()), agg.RowMID(2))
	require.Equal(t, map[uint32]struct{}{3: {}}, agg.RowSamples(0).Values)
}

func requireAggTokens(t *testing.T, agg *seq.AggregatableSamples, want ...string) {
	t.Helper()
	assert.Equal(t, seq.AggFunc(1), agg.Values.Func)
	assert.Nil(t, agg.MIDs)
	totals := []int64{}
	totalsIt := agg.Values.Totals.NewIter()
	for totalsIt.Next() {
		totals = append(totals, totalsIt.Value())
	}
	assert.Equal(t, []int64{2, 1}, totals)
	notExists := []int64{}
	notExistsIt := agg.Values.NotExists.NewIter()
	for notExistsIt.Next() {
		notExists = append(notExists, notExistsIt.Value())
	}
	assert.Equal(t, []int64{7, 8}, notExists)
	require.Equal(t, []float64{4, 1}, sumsFromColumn(agg.Values.Sums))

	var got []string
	it := agg.Tokens[0].NewIter()
	for it.Next() {
		got = append(got, string(it.Row()))
	}
	assert.Equal(t, want, got)
}

func sumsFromColumn(column seq.Float64Column) []float64 {
	var got []float64
	it := column.NewIter()
	for it.Next() {
		got = append(got, it.Value())
	}
	return got
}

func TestSearchHostProxyProtocol(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewMockStoreApiClient(ctrl)
	si := NewIngestor(Config{}, map[string]storeapi.StoreApiClient{"store-1": client})

	var seenVersions []string
	resp := &storeapi.SearchResponse{Total: 1}
	call := func(ctx context.Context, _ *storeapi.SearchRequest, _ ...grpc.CallOption) (*storeapi.SearchResponse, error) {
		md, _ := metadata.FromOutgoingContext(ctx)
		seenVersions = append(seenVersions, md.Get(consts.ProxyProtocolVersionHeader)[0])
		return resp, nil
	}
	client.EXPECT().Search(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(call).Times(1)

	got, _, err := si.searchHost(context.Background(), &storeapi.SearchRequest{}, "store-1")
	require.NoError(t, err)
	require.Equal(t, resp, got)
	require.Equal(t, []string{"2"}, seenVersions)
}

func TestStatus(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store1Mock := newStoreMock(ctx, ctrl, time.UnixMilli(999))
	store2Mock := newStoreMock(ctx, ctrl, time.UnixMilli(888))
	store3Mock := newStoreMock(ctx, ctrl, time.UnixMilli(777))
	store4Mock := newErrorStoreMock(ctx, ctrl)

	searchIngestor := NewIngestor(
		Config{
			HotReadStores: &stores.Stores{Shards: [][]string{{"store1"}, {"store2"}}},
			ReadStores:    &stores.Stores{Shards: [][]string{{"store3"}}},
			HotStores:     &stores.Stores{Shards: [][]string{{"store2"}}},
			WriteStores:   &stores.Stores{Shards: [][]string{{"store4"}, {"store5"}}},
		},
		map[string]storeapi.StoreApiClient{
			"store1": store1Mock,
			"store2": store2Mock,
			"store3": store3Mock,
			"store4": store4Mock,
		},
	)

	res := searchIngestor.Status(ctx)

	// we need to sort stores slice because requests are async an can be in any order
	sort.Slice(res.Stores, func(i, j int) bool {
		return res.Stores[i].Host < res.Stores[j].Host
	})

	oldestStorageTime := time.UnixMilli(777).UTC()

	assert.Equal(t, &IngestorStatus{
		NumberOfStores:    5,
		OldestStorageTime: &oldestStorageTime,
		Stores: []StoreStatus{
			{Host: "store1", OldestTime: time.UnixMilli(999).UTC()},
			{Host: "store2", OldestTime: time.UnixMilli(888).UTC()},
			{Host: "store3", OldestTime: time.UnixMilli(777).UTC()},
			{Host: "store4", OldestTime: time.Time{}, Error: "some error"},
			{Host: "store5", OldestTime: time.Time{}, Error: "no client for host: store5"},
		},
	}, res)
}

func TestStatusError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errorMock1 := newErrorStoreMock(ctx, ctrl)
	errorMock2 := newErrorStoreMock(ctx, ctrl)

	searchIngestor := NewIngestor(
		Config{
			HotReadStores: &stores.Stores{Shards: [][]string{{"store1"}}},
			ReadStores:    &stores.Stores{Shards: [][]string{{"store2"}}},
			HotStores:     &stores.Stores{Shards: [][]string{}},
			WriteStores:   &stores.Stores{Shards: [][]string{}},
		},
		map[string]storeapi.StoreApiClient{
			"store1": errorMock1,
			"store2": errorMock2,
		},
	)

	res := searchIngestor.Status(ctx)

	// we need to sort stores slice because requests are async an can be in any order
	sort.Slice(res.Stores, func(i, j int) bool {
		return res.Stores[i].Host < res.Stores[j].Host
	})

	assert.Equal(t, &IngestorStatus{
		NumberOfStores:    2,
		OldestStorageTime: nil,
		Stores: []StoreStatus{
			{Host: "store1", OldestTime: time.Time{}, Error: "some error"},
			{Host: "store2", OldestTime: time.Time{}, Error: "some error"},
		},
	}, res)
}

func newStoreMock(
	ctx context.Context,
	ctrl *gomock.Controller,
	oldestTime time.Time,
) *mock.MockStoreApiClient {
	m := mock.NewMockStoreApiClient(ctrl)
	m.EXPECT().
		Status(ctx, &storeapi.StatusRequest{}).
		Return(&storeapi.StatusResponse{OldestTime: timestamppb.New(oldestTime)}, nil).Times(1)
	return m
}

func newErrorStoreMock(
	ctx context.Context,
	ctrl *gomock.Controller,
) *mock.MockStoreApiClient {
	m := mock.NewMockStoreApiClient(ctrl)
	m.EXPECT().
		Status(ctx, &storeapi.StatusRequest{}).
		Return(nil, errors.New("some error")).Times(1)
	return m
}

func TestSearchTracer(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	store1Mock := mock.NewMockStoreApiClient(ctrl)
	store1Mock.EXPECT().Search(gomock.Any(), gomock.Any(), gomock.Any()).Return(&storeapi.SearchResponse{
		Explain: &storeapi.ExplainEntry{
			Message: "store/Search 1",
		},
	}, nil).Times(1)

	store2Mock := mock.NewMockStoreApiClient(ctrl)
	store2Mock.EXPECT().Search(gomock.Any(), gomock.Any(), gomock.Any()).Return(&storeapi.SearchResponse{
		Explain: &storeapi.ExplainEntry{
			Message: "store/Search 2",
		},
	}, nil).Times(1)

	store3Mock := mock.NewMockStoreApiClient(ctrl)
	store3Mock.EXPECT().Search(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("some error")).Times(1)

	searchIngestor := NewIngestor(
		Config{
			HotReadStores: &stores.Stores{Shards: [][]string{{"store1"}, {"store2"}, {"store3"}}},
			ReadStores:    &stores.Stores{Shards: [][]string{}},
			HotStores:     &stores.Stores{Shards: [][]string{}},
			WriteStores:   &stores.Stores{Shards: [][]string{}},
		},
		map[string]storeapi.StoreApiClient{
			"store1": store1Mock,
			"store2": store2Mock,
			"store3": store3Mock,
		},
	)

	tr := querytracer.New(true, "test")
	_, _, _, _ = searchIngestor.Search(ctx, &SearchRequest{Q: []byte("*"), Explain: true, Size: 10}, tr)
	tr.Done()

	expected := &querytracer.Span{
		Message: "test",
		Children: []*querytracer.Span{
			{
				Message: "proxy/searchShard",
				Children: []*querytracer.Span{
					{Message: "Making search request to store1"},
					{Message: "store/Search 1"},
				},
			},
			{
				Message: "proxy/searchShard",
				Children: []*querytracer.Span{
					{Message: "Making search request to store2"},
					{Message: "store/Search 2"},
				},
			},
			{
				Message: "proxy/searchShard",
				Children: []*querytracer.Span{
					{Message: "Making search request to store3"},
					{Message: "Got error from store3: some error"},
				},
			},
		},
	}

	assert.Equal(t, expected, zeroDurationsInSpan(tr.ToSpan()))
}

func zeroDurationsInSpan(s *querytracer.Span) *querytracer.Span {
	s.Duration = 0
	for _, c := range s.Children {
		zeroDurationsInSpan(c)
	}
	return s
}
