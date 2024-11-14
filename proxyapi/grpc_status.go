package proxyapi

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
)

func (g *grpcV1) Status(ctx context.Context, _ *seqproxyapi.StatusRequest) (*seqproxyapi.StatusResponse, error) {
	ingestorStatus := g.searchIngestor.Status(ctx)

	var oldestStorageTime *timestamppb.Timestamp
	if ingestorStatus.OldestStorageTime != nil {
		oldestStorageTime = timestamppb.New(*ingestorStatus.OldestStorageTime)
	}

	return &seqproxyapi.StatusResponse{
		NumberOfStores:    int32(ingestorStatus.NumberOfStores),
		OldestStorageTime: oldestStorageTime,
		Stores:            convertStores(ingestorStatus.Stores),
	}, nil
}

func convertStores(in []search.StoreStatus) []*seqproxyapi.StoreStatus {
	out := make([]*seqproxyapi.StoreStatus, len(in))
	for i, hs := range in {
		out[i] = &seqproxyapi.StoreStatus{Host: hs.Host}

		if hs.Error != "" {
			out[i].Error = &hs.Error
			continue
		}

		out[i].Values = &seqproxyapi.StoreStatusValues{
			OldestTime: timestamppb.New(hs.OldestTime),
		}
	}

	return out
}
