package search

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search/mock"
	"github.com/ozontech/seq-db/proxy/stores"
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
