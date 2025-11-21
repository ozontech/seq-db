package grpcutil

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func TraceUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		h grpc.UnaryHandler,
	) (_ interface{}, err error) {
		ctx, span := tracing.StartSpan(ctx, info.FullMethod)
		defer span.End()
		resp, err := h(ctx, req)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				st = status.New(codes.Unknown, err.Error())
				err = status.Error(codes.Unknown, err.Error())
			}
			span.SetStatus(trace.Status{
				Code:    int32(st.Code()),
				Message: err.Error(),
			})
		}
		return resp, err
	}
}

func RecoverUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		h grpc.UnaryHandler,
	) (_ interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				recErr := fmt.Errorf(
					"%s request failed (recovered after panic): %v",
					info.FullMethod,
					r,
				)
				util.Recover(metric.IngestorPanics, recErr)
				err = status.Error(codes.Internal, recErr.Error())
			}
		}()
		resp, err := h(ctx, req)
		return resp, err
	}
}

func RecoverStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo,
		h grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				recErr := fmt.Errorf(
					"%s request failed (recovered after panic): %v",
					info.FullMethod,
					r,
				)
				util.Recover(metric.IngestorPanics, recErr)
				err = status.Error(codes.Internal, recErr.Error())
			}
		}()
		err = h(srv, ss)
		return err
	}
}

func LogUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		h grpc.UnaryHandler,
	) (interface{}, error) {
		rBody, err := protojson.Marshal(req.(proto.Message))
		if err != nil {
			logger.Error("failed to marshal request message", zap.Error(err))
		}
		logger.Info("incoming request",
			zap.String("method", info.FullMethod),
			zap.String("body", string(rBody)),
		)
		resp, err := h(ctx, req)
		return resp, err
	}
}

func LogStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo,
		h grpc.StreamHandler,
	) error {
		logger.Info("incoming request",
			zap.String("method", info.FullMethod),
		)
		return h(srv, ss)
	}
}

// PassMetadataUnaryClientInterceptor passes metadata from incoming context to outgoing context.
func PassMetadataUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// MIDPrecisionHeaderUnaryServerInterceptor sets the MID precision header for all unary responses.
func MIDPrecisionHeaderUnaryServerInterceptor(precision seq.MIDPrecision) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		h grpc.UnaryHandler,
	) (interface{}, error) {
		md := metadata.New(map[string]string{
			consts.MIDPrecisionHeader: precision.String(),
		})
		if err := grpc.SetHeader(ctx, md); err != nil {
			logger.Error("failed to set MID precision header", zap.Error(err))
		}
		return h(ctx, req)
	}
}

// MIDPrecisionHeaderStreamServerInterceptor sets the MID precision header for all streaming responses.
func MIDPrecisionHeaderStreamServerInterceptor(precision seq.MIDPrecision) grpc.StreamServerInterceptor {
	return func(
		srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo,
		h grpc.StreamHandler,
	) error {
		md := metadata.New(map[string]string{
			consts.MIDPrecisionHeader: precision.String(),
		})
		if err := ss.SendHeader(md); err != nil {
			logger.Error("failed to send MID precision header in stream", zap.Error(err))
		}
		return h(srv, ss)
	}
}
