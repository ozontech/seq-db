package grpcutil

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	encodingproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/protobuf/proto"
)

type VTProtoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

// CodecName uses default codec name to override default codec.
const CodecName = encodingproto.Name

// VTProtoCodec implements the Codec interface.
// It will use the codec vtproto, if possible, otherwise falls on the standard codec.
// This is necessary since not all structures implement vtproto codec (for example emptypb.Empty).
//
// See: https://github.com/planetscale/vtprotobuf/tree/v0.6.0?tab=readme-ov-file#mixing-protobuf-implementations-with-grpc
type VTProtoCodec struct{}

var _ encoding.Codec = VTProtoCodec{}

func (VTProtoCodec) Marshal(v any) ([]byte, error) {
	if vtMessage, ok := v.(VTProtoMessage); ok {
		return vtMessage.MarshalVT()
	}
	// copy-pasted implementation of the default codec
	if vv, ok := v.(proto.Message); ok {
		return proto.Marshal(vv)
	}

	return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message or grpcutil.VTProtoMessage", v)
}

func (VTProtoCodec) Unmarshal(data []byte, v any) error {
	if vtMessage, ok := v.(VTProtoMessage); ok {
		return vtMessage.UnmarshalVT(data)
	}
	// copy-pasted implementation of the default codec
	vv, ok := v.(proto.Message)
	if ok {
		return proto.Unmarshal(data, vv)
	}
	return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message or grpcutil.VTProtoMessage", v)
}

func (VTProtoCodec) Name() string {
	return CodecName
}

type poolable interface {
	ReturnToVTPool()
}

// ReturnToVTPoolUnaryServerInterceptor calls ReturnToVTPool for each request that implements this method.
// It is necessary because vtproto does not return objects to the pool.
func ReturnToVTPoolUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		if reqPoolable, ok := req.(poolable); ok {
			reqPoolable.ReturnToVTPool()
		}
		return resp, err
	}
}
