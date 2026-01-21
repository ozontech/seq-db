package storeapi

import (
	"context"
	"io"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/storeapi"
)

type inMemoryAPIClient struct {
	store *Store
}

func NewClient(store *Store) storeapi.StoreApiClient {
	return &inMemoryAPIClient{store: store}
}

func (i inMemoryAPIClient) Bulk(ctx context.Context, in *storeapi.BulkRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	// NOTE: We copy `Metas` to prevent dataraces because `store` might work
	// with this memory even when it returned response to client.
	in.Metas = slices.Clone(in.Metas)
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().Bulk(ctx, in)
}

func (i inMemoryAPIClient) Search(ctx context.Context, in *storeapi.SearchRequest, opts ...grpc.CallOption) (*storeapi.SearchResponse, error) {
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().Search(ctx, in)
}

func (i inMemoryAPIClient) StartAsyncSearch(ctx context.Context, in *storeapi.StartAsyncSearchRequest, opts ...grpc.CallOption) (*storeapi.StartAsyncSearchResponse, error) {
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().StartAsyncSearch(ctx, in)
}

func (i inMemoryAPIClient) FetchAsyncSearchResult(ctx context.Context, in *storeapi.FetchAsyncSearchResultRequest, opts ...grpc.CallOption) (*storeapi.FetchAsyncSearchResultResponse, error) {
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().FetchAsyncSearchResult(ctx, in)
}

func (i inMemoryAPIClient) CancelAsyncSearch(ctx context.Context, in *storeapi.CancelAsyncSearchRequest, opts ...grpc.CallOption) (*storeapi.CancelAsyncSearchResponse, error) {
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().CancelAsyncSearch(ctx, in)
}

func (i inMemoryAPIClient) DeleteAsyncSearch(ctx context.Context, in *storeapi.DeleteAsyncSearchRequest, opts ...grpc.CallOption) (*storeapi.DeleteAsyncSearchResponse, error) {
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().DeleteAsyncSearch(ctx, in)
}

func (i inMemoryAPIClient) GetAsyncSearchesList(ctx context.Context, in *storeapi.GetAsyncSearchesListRequest, opts ...grpc.CallOption) (*storeapi.GetAsyncSearchesListResponse, error) {
	setProtocolVersionHeader(opts...)
	return i.store.GrpcV1().GetAsyncSearchesList(ctx, in)
}

func setProtocolVersionHeader(opts ...grpc.CallOption) {
	for _, opt := range opts {
		if headerOpt, ok := opt.(grpc.HeaderCallOption); ok && headerOpt.HeaderAddr != nil {
			if *headerOpt.HeaderAddr == nil {
				*headerOpt.HeaderAddr = make(metadata.MD)
			}
			(*headerOpt.HeaderAddr)[consts.StoreProtocolVersionHeader] = []string{config.StoreProtocolVersion2.String()}
		}
	}
}

type storeAPIFetchServer struct {
	grpc.ServerStream
	ctx context.Context
	buf []*storeapi.BinaryData
}

func newStoreAPIFetchServer(ctx context.Context) *storeAPIFetchServer {
	return &storeAPIFetchServer{ctx: ctx}
}

func (x *storeAPIFetchServer) Send(m *storeapi.BinaryData) error {
	x.buf = append(x.buf, m.CloneVT())
	return nil
}

func (x *storeAPIFetchServer) Context() context.Context {
	return x.ctx
}

type storeAPIFetchClient struct {
	grpc.ClientStream
	buf     []*storeapi.BinaryData
	readPos int
}

func newStoreAPIFetchClient(b []*storeapi.BinaryData) *storeAPIFetchClient {
	return &storeAPIFetchClient{buf: b}
}

func (x *storeAPIFetchClient) Header() (metadata.MD, error) {
	md := make(metadata.MD)
	md[consts.StoreProtocolVersionHeader] = []string{config.StoreProtocolVersion2.String()}
	return md, nil
}

func (x *storeAPIFetchClient) Recv() (*storeapi.BinaryData, error) {
	if x.readPos >= len(x.buf) {
		return nil, io.EOF
	}

	res := x.buf[x.readPos]
	x.readPos++

	return res, nil
}

func (i inMemoryAPIClient) Fetch(ctx context.Context, in *storeapi.FetchRequest, opts ...grpc.CallOption) (storeapi.StoreApi_FetchClient, error) {
	s := newStoreAPIFetchServer(ctx)
	setProtocolVersionHeader(opts...)
	if err := i.store.GrpcV1().Fetch(in, s); err != nil {
		return nil, err
	}
	return newStoreAPIFetchClient(s.buf), nil
}

func (i inMemoryAPIClient) Status(ctx context.Context, in *storeapi.StatusRequest, _ ...grpc.CallOption) (*storeapi.StatusResponse, error) {
	return i.store.GrpcV1().Status(ctx, in)
}
