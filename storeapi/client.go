package storeapi

import (
	"context"
	"io"
	"slices"
	"sync"

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

// streamSearchPipe is the shared channel plumbing connecting the in-memory
// StreamSearch client with the StreamSearch server handler.
// The server handler runs in a goroutine started by inMemoryAPIClient.StreamSearch.
type streamSearchPipe struct {
	ctx context.Context

	// reqCh carries StreamSearchRequest messages from client to server.
	reqCh chan *storeapi.StreamSearchRequest
	// resCh carries StreamSearchResponse messages from server to client.
	resCh chan *storeapi.StreamSearchResponse

	// reqClosed is closed when the client will send no more requests, so the
	// server's Recv returns io.EOF. Closed at most once.
	reqClosed chan struct{}
	closeOnce sync.Once

	// serverDone is closed when the server handler returns; serverErr holds its
	// result. The client reads them after resCh is closed.
	serverDone chan struct{}
	serverErr  error
}

func newStreamSearchPipe(ctx context.Context) *streamSearchPipe {
	return &streamSearchPipe{
		ctx:        ctx,
		reqCh:      make(chan *storeapi.StreamSearchRequest, 1),
		resCh:      make(chan *storeapi.StreamSearchResponse, 1),
		reqClosed:  make(chan struct{}),
		serverDone: make(chan struct{}),
	}
}

func (p *streamSearchPipe) closeReq() {
	p.closeOnce.Do(func() { close(p.reqClosed) })
}

type storeAPIStreamSearchServer struct {
	grpc.ServerStream
	*streamSearchPipe
}

func (s storeAPIStreamSearchServer) Send(m *storeapi.StreamSearchResponse) error {
	select {
	case s.resCh <- m.CloneVT():
		return nil
	case <-s.serverDone:
		return io.EOF
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s storeAPIStreamSearchServer) Recv() (*storeapi.StreamSearchRequest, error) {
	select {
	case m, ok := <-s.reqCh:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	case <-s.reqClosed:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s storeAPIStreamSearchServer) Context() context.Context { return s.ctx }

type storeAPIStreamSearchClient struct {
	grpc.ClientStream
	*streamSearchPipe
}

func (c *storeAPIStreamSearchClient) Header() (metadata.MD, error) {
	md := make(metadata.MD)
	md[consts.StoreProtocolVersionHeader] = []string{config.StoreProtocolVersion2.String()}
	return md, nil
}

func (c *storeAPIStreamSearchClient) Context() context.Context { return c.ctx }

func (c *storeAPIStreamSearchClient) CloseSend() error { return nil }

func (c *storeAPIStreamSearchClient) Send(m *storeapi.StreamSearchRequest) error {
	select {
	case c.reqCh <- m.CloneVT():
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func (c *storeAPIStreamSearchClient) Recv() (*storeapi.StreamSearchResponse, error) {
	// If the server handler has already finished, we need to drain whatever it produced before cancelling the context.
	select {
	case m, ok := <-c.resCh:
		if !ok {
			return nil, c.waitServerErr()
		}
		return m, nil
	default:
	}

	select {
	case m, ok := <-c.resCh:
		if !ok {
			return nil, c.waitServerErr()
		}
		return m, nil
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}

func (c *storeAPIStreamSearchClient) waitServerErr() error {
	<-c.serverDone
	if c.serverErr != nil {
		return c.serverErr
	}
	return io.EOF
}

func (i inMemoryAPIClient) StreamSearch(ctx context.Context, opts ...grpc.CallOption) (storeapi.StoreApi_StreamSearchClient, error) {
	setProtocolVersionHeader(opts...)
	pipeCtx, cancel := context.WithCancel(ctx)
	p := newStreamSearchPipe(pipeCtx)

	go func() {
		defer cancel()
		// Closing resCh unblocks the client's Recv with io.EOF (or serverErr).
		// closeReq drains the request side so the handler's Recv does not block.
		defer p.closeReq()
		defer close(p.resCh)
		defer close(p.serverDone)
		p.serverErr = i.store.GrpcV1().StreamSearch(storeAPIStreamSearchServer{streamSearchPipe: p})
	}()

	return &storeAPIStreamSearchClient{streamSearchPipe: p}, nil
}
