package tracing

import (
	"context"

	"github.com/ozontech/seq-db/consts"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

// ClientHandler injects debugging meta
type ClientHandler struct {
	ocgrpc.ClientHandler
}

// ServerHandler parses debugging meta
type ServerHandler struct {
	ocgrpc.ServerHandler
}

func (h *ClientHandler) TagRPC(ctx context.Context, s *stats.RPCTagInfo) context.Context {
	ctx = h.ClientHandler.TagRPC(ctx, s)
	if key := ctx.Value(consts.JaegerDebugKey); key != nil {
		ctx = metadata.AppendToOutgoingContext(ctx, []string{consts.JaegerDebugKey, "true"}...)
	}
	return ctx
}

func (h *ClientHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	if span := trace.FromContext(ctx); span != nil {
		if key := ctx.Value(consts.JaegerDebugKey); key != nil {
			span.AddAttributes(trace.BoolAttribute(consts.JaegerDebugKey, true))
		}
	}
	h.ClientHandler.HandleRPC(ctx, rs)
}

func (h *ServerHandler) TagRPC(ctx context.Context, s *stats.RPCTagInfo) context.Context {
	ctx = h.ServerHandler.TagRPC(ctx, s)
	md, _ := metadata.FromIncomingContext(ctx)
	if traceContext, ok := md[consts.JaegerDebugKey]; ok && len(traceContext) == 1 {
		ctx = context.WithValue(ctx, consts.JaegerDebugKey, true)
	}
	return ctx
}

func (h *ServerHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	if span := trace.FromContext(ctx); span != nil {
		if key := ctx.Value(consts.JaegerDebugKey); key != nil {
			span.AddAttributes(trace.BoolAttribute(consts.JaegerDebugKey, true))
		}
	}
	h.ServerHandler.HandleRPC(ctx, rs)
}
