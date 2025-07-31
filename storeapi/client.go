package storeapi

import (
	"context"
	"io"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ozontech/seq-db/pkg/storeapi"
)

type inMemoryAPIClient struct {
	store *Store
}

func NewClient(store *Store) storeapi.StoreApiClient {
	return &inMemoryAPIClient{store: store}
}

func (i inMemoryAPIClient) Bulk(ctx context.Context, in *storeapi.BulkRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	// NOTE: We copy `Metas` to prevent dataraces because `store` might work
	// with this memory even when it returned response to client.
	in.Metas = slices.Clone(in.Metas)
	return i.store.GrpcV1().Bulk(ctx, in)
}

func (i inMemoryAPIClient) Search(ctx context.Context, in *storeapi.SearchRequest, _ ...grpc.CallOption) (*storeapi.SearchResponse, error) {
	return i.store.GrpcV1().Search(ctx, in)
}

func (i inMemoryAPIClient) StartAsyncSearch(ctx context.Context, in *storeapi.StartAsyncSearchRequest, _ ...grpc.CallOption) (*storeapi.StartAsyncSearchResponse, error) {
	return i.store.GrpcV1().StartAsyncSearch(ctx, in)
}

func (i inMemoryAPIClient) FetchAsyncSearchResult(ctx context.Context, in *storeapi.FetchAsyncSearchResultRequest, _ ...grpc.CallOption) (*storeapi.FetchAsyncSearchResultResponse, error) {
	return i.store.GrpcV1().FetchAsyncSearchResult(ctx, in)
}

func (i inMemoryAPIClient) CancelAsyncSearch(ctx context.Context, in *storeapi.CancelAsyncSearchRequest, _ ...grpc.CallOption) (*storeapi.CancelAsyncSearchResponse, error) {
	return i.store.GrpcV1().CancelAsyncSearch(ctx, in)
}

func (i inMemoryAPIClient) DeleteAsyncSearch(ctx context.Context, in *storeapi.DeleteAsyncSearchRequest, _ ...grpc.CallOption) (*storeapi.DeleteAsyncSearchResponse, error) {
	return i.store.GrpcV1().DeleteAsyncSearch(ctx, in)
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

func (x *storeAPIFetchClient) Recv() (*storeapi.BinaryData, error) {
	if x.readPos >= len(x.buf) {
		return nil, io.EOF
	}

	res := x.buf[x.readPos]
	x.readPos++

	return res, nil
}

func (i inMemoryAPIClient) Fetch(ctx context.Context, in *storeapi.FetchRequest, _ ...grpc.CallOption) (storeapi.StoreApi_FetchClient, error) {
	s := newStoreAPIFetchServer(ctx)
	if err := i.store.GrpcV1().Fetch(in, s); err != nil {
		return nil, err
	}
	return newStoreAPIFetchClient(s.buf), nil
}

func (i inMemoryAPIClient) Status(ctx context.Context, in *storeapi.StatusRequest, _ ...grpc.CallOption) (*storeapi.StatusResponse, error) {
	return i.store.GrpcV1().Status(ctx, in)
}
