package storeapi

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/pkg/storeapi"
)

func (g *GrpcV1) Status(_ context.Context, _ *storeapi.StatusRequest) (*storeapi.StatusResponse, error) {
	oldestFracTime := g.fracManager.OldestCT.Load()

	return &storeapi.StatusResponse{
		OldestTime: timestamppb.New(time.UnixMilli(int64(oldestFracTime))),
	}, nil
}
