// Code generated by protoc-gen-grpc-gateway. DO NOT EDIT.
// source: storeapi/store_api.proto

/*
Package storeapi is a reverse proxy.

It translates gRPC into RESTful JSON APIs.
*/
package storeapi

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Suppress "imported and not used" errors
var (
	_ codes.Code
	_ io.Reader
	_ status.Status
	_ = errors.New
	_ = runtime.String
	_ = utilities.NewDoubleArray
	_ = metadata.Join
)

func request_StoreApi_Bulk_0(ctx context.Context, marshaler runtime.Marshaler, client StoreApiClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq BulkRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := client.Bulk(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err
}

func local_request_StoreApi_Bulk_0(ctx context.Context, marshaler runtime.Marshaler, server StoreApiServer, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq BulkRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := server.Bulk(ctx, &protoReq)
	return msg, metadata, err
}

func request_StoreApi_Search_0(ctx context.Context, marshaler runtime.Marshaler, client StoreApiClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq SearchRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := client.Search(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err
}

func local_request_StoreApi_Search_0(ctx context.Context, marshaler runtime.Marshaler, server StoreApiServer, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq SearchRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := server.Search(ctx, &protoReq)
	return msg, metadata, err
}

func request_StoreApi_StartAsyncSearch_0(ctx context.Context, marshaler runtime.Marshaler, client StoreApiClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq StartAsyncSearchRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := client.StartAsyncSearch(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err
}

func local_request_StoreApi_StartAsyncSearch_0(ctx context.Context, marshaler runtime.Marshaler, server StoreApiServer, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq StartAsyncSearchRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := server.StartAsyncSearch(ctx, &protoReq)
	return msg, metadata, err
}

func request_StoreApi_FetchAsyncSearchResult_0(ctx context.Context, marshaler runtime.Marshaler, client StoreApiClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq FetchAsyncSearchResultRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := client.FetchAsyncSearchResult(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err
}

func local_request_StoreApi_FetchAsyncSearchResult_0(ctx context.Context, marshaler runtime.Marshaler, server StoreApiServer, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq FetchAsyncSearchResultRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := server.FetchAsyncSearchResult(ctx, &protoReq)
	return msg, metadata, err
}

func request_StoreApi_Fetch_0(ctx context.Context, marshaler runtime.Marshaler, client StoreApiClient, req *http.Request, pathParams map[string]string) (StoreApi_FetchClient, runtime.ServerMetadata, error) {
	var (
		protoReq FetchRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	stream, err := client.Fetch(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil
}

func request_StoreApi_Status_0(ctx context.Context, marshaler runtime.Marshaler, client StoreApiClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq StatusRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := client.Status(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err
}

func local_request_StoreApi_Status_0(ctx context.Context, marshaler runtime.Marshaler, server StoreApiServer, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var (
		protoReq StatusRequest
		metadata runtime.ServerMetadata
	)
	if err := marshaler.NewDecoder(req.Body).Decode(&protoReq); err != nil && !errors.Is(err, io.EOF) {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	msg, err := server.Status(ctx, &protoReq)
	return msg, metadata, err
}

// RegisterStoreApiHandlerServer registers the http handlers for service StoreApi to "mux".
// UnaryRPC     :call StoreApiServer directly.
// StreamingRPC :currently unsupported pending https://github.com/grpc/grpc-go/issues/906.
// Note that using this registration option will cause many gRPC library features to stop working. Consider using RegisterStoreApiHandlerFromEndpoint instead.
// GRPC interceptors will not work for this type of registration. To use interceptors, you must use the "runtime.WithMiddlewares" option in the "runtime.NewServeMux" call.
func RegisterStoreApiHandlerServer(ctx context.Context, mux *runtime.ServeMux, server StoreApiServer) error {
	mux.Handle(http.MethodPost, pattern_StoreApi_Bulk_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		var stream runtime.ServerTransportStream
		ctx = grpc.NewContextWithServerTransportStream(ctx, &stream)
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateIncomingContext(ctx, mux, req, "/api.StoreApi/Bulk", runtime.WithHTTPPathPattern("/api.StoreApi/Bulk"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := local_request_StoreApi_Bulk_0(annotatedContext, inboundMarshaler, server, req, pathParams)
		md.HeaderMD, md.TrailerMD = metadata.Join(md.HeaderMD, stream.Header()), metadata.Join(md.TrailerMD, stream.Trailer())
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Bulk_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_Search_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		var stream runtime.ServerTransportStream
		ctx = grpc.NewContextWithServerTransportStream(ctx, &stream)
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateIncomingContext(ctx, mux, req, "/api.StoreApi/Search", runtime.WithHTTPPathPattern("/api.StoreApi/Search"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := local_request_StoreApi_Search_0(annotatedContext, inboundMarshaler, server, req, pathParams)
		md.HeaderMD, md.TrailerMD = metadata.Join(md.HeaderMD, stream.Header()), metadata.Join(md.TrailerMD, stream.Trailer())
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Search_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_StartAsyncSearch_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		var stream runtime.ServerTransportStream
		ctx = grpc.NewContextWithServerTransportStream(ctx, &stream)
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateIncomingContext(ctx, mux, req, "/api.StoreApi/StartAsyncSearch", runtime.WithHTTPPathPattern("/api.StoreApi/StartAsyncSearch"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := local_request_StoreApi_StartAsyncSearch_0(annotatedContext, inboundMarshaler, server, req, pathParams)
		md.HeaderMD, md.TrailerMD = metadata.Join(md.HeaderMD, stream.Header()), metadata.Join(md.TrailerMD, stream.Trailer())
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_StartAsyncSearch_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_FetchAsyncSearchResult_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		var stream runtime.ServerTransportStream
		ctx = grpc.NewContextWithServerTransportStream(ctx, &stream)
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateIncomingContext(ctx, mux, req, "/api.StoreApi/FetchAsyncSearchResult", runtime.WithHTTPPathPattern("/api.StoreApi/FetchAsyncSearchResult"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := local_request_StoreApi_FetchAsyncSearchResult_0(annotatedContext, inboundMarshaler, server, req, pathParams)
		md.HeaderMD, md.TrailerMD = metadata.Join(md.HeaderMD, stream.Header()), metadata.Join(md.TrailerMD, stream.Trailer())
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_FetchAsyncSearchResult_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})

	mux.Handle(http.MethodPost, pattern_StoreApi_Fetch_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		err := status.Error(codes.Unimplemented, "streaming calls are not yet supported in the in-process transport")
		_, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
		return
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_Status_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		var stream runtime.ServerTransportStream
		ctx = grpc.NewContextWithServerTransportStream(ctx, &stream)
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateIncomingContext(ctx, mux, req, "/api.StoreApi/Status", runtime.WithHTTPPathPattern("/api.StoreApi/Status"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := local_request_StoreApi_Status_0(annotatedContext, inboundMarshaler, server, req, pathParams)
		md.HeaderMD, md.TrailerMD = metadata.Join(md.HeaderMD, stream.Header()), metadata.Join(md.TrailerMD, stream.Trailer())
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Status_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})

	return nil
}

// RegisterStoreApiHandlerFromEndpoint is same as RegisterStoreApiHandler but
// automatically dials to "endpoint" and closes the connection when "ctx" gets done.
func RegisterStoreApiHandlerFromEndpoint(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error) {
	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Errorf("Failed to close conn to %s: %v", endpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Errorf("Failed to close conn to %s: %v", endpoint, cerr)
			}
		}()
	}()
	return RegisterStoreApiHandler(ctx, mux, conn)
}

// RegisterStoreApiHandler registers the http handlers for service StoreApi to "mux".
// The handlers forward requests to the grpc endpoint over "conn".
func RegisterStoreApiHandler(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return RegisterStoreApiHandlerClient(ctx, mux, NewStoreApiClient(conn))
}

// RegisterStoreApiHandlerClient registers the http handlers for service StoreApi
// to "mux". The handlers forward requests to the grpc endpoint over the given implementation of "StoreApiClient".
// Note: the gRPC framework executes interceptors within the gRPC handler. If the passed in "StoreApiClient"
// doesn't go through the normal gRPC flow (creating a gRPC client etc.) then it will be up to the passed in
// "StoreApiClient" to call the correct interceptors. This client ignores the HTTP middlewares.
func RegisterStoreApiHandlerClient(ctx context.Context, mux *runtime.ServeMux, client StoreApiClient) error {
	mux.Handle(http.MethodPost, pattern_StoreApi_Bulk_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, req, "/api.StoreApi/Bulk", runtime.WithHTTPPathPattern("/api.StoreApi/Bulk"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_StoreApi_Bulk_0(annotatedContext, inboundMarshaler, client, req, pathParams)
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Bulk_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_Search_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, req, "/api.StoreApi/Search", runtime.WithHTTPPathPattern("/api.StoreApi/Search"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_StoreApi_Search_0(annotatedContext, inboundMarshaler, client, req, pathParams)
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Search_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_StartAsyncSearch_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, req, "/api.StoreApi/StartAsyncSearch", runtime.WithHTTPPathPattern("/api.StoreApi/StartAsyncSearch"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_StoreApi_StartAsyncSearch_0(annotatedContext, inboundMarshaler, client, req, pathParams)
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_StartAsyncSearch_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_FetchAsyncSearchResult_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, req, "/api.StoreApi/FetchAsyncSearchResult", runtime.WithHTTPPathPattern("/api.StoreApi/FetchAsyncSearchResult"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_StoreApi_FetchAsyncSearchResult_0(annotatedContext, inboundMarshaler, client, req, pathParams)
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_FetchAsyncSearchResult_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_Fetch_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, req, "/api.StoreApi/Fetch", runtime.WithHTTPPathPattern("/api.StoreApi/Fetch"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_StoreApi_Fetch_0(annotatedContext, inboundMarshaler, client, req, pathParams)
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Fetch_0(annotatedContext, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)
	})
	mux.Handle(http.MethodPost, pattern_StoreApi_Status_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		annotatedContext, err := runtime.AnnotateContext(ctx, mux, req, "/api.StoreApi/Status", runtime.WithHTTPPathPattern("/api.StoreApi/Status"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_StoreApi_Status_0(annotatedContext, inboundMarshaler, client, req, pathParams)
		annotatedContext = runtime.NewServerMetadataContext(annotatedContext, md)
		if err != nil {
			runtime.HTTPError(annotatedContext, mux, outboundMarshaler, w, req, err)
			return
		}
		forward_StoreApi_Status_0(annotatedContext, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)
	})
	return nil
}

var (
	pattern_StoreApi_Bulk_0                   = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"api.StoreApi", "Bulk"}, ""))
	pattern_StoreApi_Search_0                 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"api.StoreApi", "Search"}, ""))
	pattern_StoreApi_StartAsyncSearch_0       = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"api.StoreApi", "StartAsyncSearch"}, ""))
	pattern_StoreApi_FetchAsyncSearchResult_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"api.StoreApi", "FetchAsyncSearchResult"}, ""))
	pattern_StoreApi_Fetch_0                  = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"api.StoreApi", "Fetch"}, ""))
	pattern_StoreApi_Status_0                 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"api.StoreApi", "Status"}, ""))
)

var (
	forward_StoreApi_Bulk_0                   = runtime.ForwardResponseMessage
	forward_StoreApi_Search_0                 = runtime.ForwardResponseMessage
	forward_StoreApi_StartAsyncSearch_0       = runtime.ForwardResponseMessage
	forward_StoreApi_FetchAsyncSearchResult_0 = runtime.ForwardResponseMessage
	forward_StoreApi_Fetch_0                  = runtime.ForwardResponseStream
	forward_StoreApi_Status_0                 = runtime.ForwardResponseMessage
)
