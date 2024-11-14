package proxyapi

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxyapi/mock"
)

var errorMessage = "some error"

func TestGrpcV1_Status(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	oldestStorageTime := time.UnixMilli(777)

	siMock := mock.NewMockSearchIngestor(ctrl)
	siMock.EXPECT().Status(ctx).Return(&search.IngestorStatus{
		NumberOfStores:    4,
		OldestStorageTime: &oldestStorageTime,
		Stores: []search.StoreStatus{
			{Host: "store1", OldestTime: time.UnixMilli(999)},
			{Host: "store2", OldestTime: time.UnixMilli(888)},
			{Host: "store3", OldestTime: time.UnixMilli(777)},
			{Host: "store4", Error: errorMessage},
		},
	}).Times(1)

	s := newGrpcV1(APIConfig{}, siMock, nil, nil, nil)
	got, err := s.Status(ctx, &seqproxyapi.StatusRequest{})
	require.NoError(t, err)
	assert.Equal(t, &seqproxyapi.StatusResponse{
		NumberOfStores:    4,
		OldestStorageTime: timestamppb.New(time.UnixMilli(777)),
		Stores: []*seqproxyapi.StoreStatus{
			{
				Host:   "store1",
				Values: &seqproxyapi.StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(999))},
			},
			{
				Host:   "store2",
				Values: &seqproxyapi.StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(888))},
			},
			{
				Host:   "store3",
				Values: &seqproxyapi.StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(777))},
			},
			{
				Host:  "store4",
				Error: &errorMessage,
			},
		},
	}, got)
}

func TestGrpcV1_StatusError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	siMock := mock.NewMockSearchIngestor(ctrl)
	siMock.EXPECT().Status(ctx).Return(&search.IngestorStatus{
		NumberOfStores:    1,
		OldestStorageTime: nil,
		Stores: []search.StoreStatus{
			{Host: "store1", Error: errorMessage},
		},
	}).Times(1)

	s := newGrpcV1(APIConfig{}, siMock, nil, nil, nil)
	got, err := s.Status(ctx, &seqproxyapi.StatusRequest{})
	require.NoError(t, err)
	assert.Equal(t, &seqproxyapi.StatusResponse{
		NumberOfStores:    1,
		OldestStorageTime: nil,
		Stores: []*seqproxyapi.StoreStatus{
			{
				Host:  "store1",
				Error: &errorMessage,
			},
		},
	}, got)
}
