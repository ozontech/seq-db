package proxyapi

import (
	"context"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
)

func (g *grpcV1) Mapping(_ context.Context, _ *seqproxyapi.MappingRequest) (*seqproxyapi.MappingResponse, error) {
	mapping := g.mapping.GetRawMappingBytes()
	return &seqproxyapi.MappingResponse{Data: mapping}, nil
}
