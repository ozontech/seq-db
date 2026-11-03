package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/fracmanager"
	storeapiclient "github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storeapi"
)

var grpcMaxCallOpts = []grpc.CallOption{
	grpc.MaxCallRecvMsgSize(bulk.MaxCallRecvMsgSize),
	grpc.MaxCallSendMsgSize(bulk.MaxCallSendMsgSize),
}

type staticMappingProvider struct {
	mapping seq.Mapping
}

func (p staticMappingProvider) GetMapping() seq.Mapping {
	return p.mapping
}

func newTestStore(t *testing.T, mapping seq.Mapping) (storeapiclient.StoreApiClient, *storeapi.Store) {
	t.Helper()

	dataDir := filepath.Join(t.TempDir(), "store")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	cfg := storeapi.StoreConfig{
		FracManager: *fracmanager.FillConfigWithDefault(&fracmanager.Config{
			DataDir:   dataDir,
			FracSize:  16 * uint64(units.MiB),
			TotalSize: 1 * uint64(units.GiB),
			SealParams: common.SealParams{
				DocBlockSize: int(units.MiB),
			},
		}),
		API: storeapi.APIConfig{
			Search: storeapi.SearchConfig{
				WorkersCount:          2,
				FractionsPerIteration: 2,
			},
		},
	}

	store, err := storeapi.NewStore(context.Background(), cfg, nil, staticMappingProvider{mapping}, nil)
	require.NoError(t, err)
	t.Cleanup(store.Stop)

	return storeapi.NewClient(store), store
}

func TestStoreSender(t *testing.T) {
	docs := [][]byte{
		[]byte(`{"timestamp":"2000-01-01T13:00:25Z","message":"first doc","level":"1"}`),
		[]byte(`{"timestamp":"2000-01-01T13:00:32Z","message":"second doc"}`),
		[]byte(`{"timestamp":"2000-01-01T13:00:43Z","message":"third doc","level":"2"}`),
	}

	tests := []struct {
		name      string
		query     string
		wantTotal uint64
	}{
		{"finds by text field", "message:second", 1},
		{"finds by keyword field", "level:1", 1},
		{"finds nothing for missing token", "message:missing", 0},
	}

	mapping := seq.TestMapping
	mapping["level"] = seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0)

	client, st := newTestStore(t, mapping)
	s := newStoreSenderWithClient(client, mapping)

	require.NoError(t, s.Send(context.Background(), docs))
	st.WaitIdle()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.client.Search(context.Background(), &storeapiclient.SearchRequest{
				Query:     tt.query,
				From:      0,
				To:        1<<62 - 1,
				Size:      10,
				WithTotal: true,
			}, grpcMaxCallOpts...)
			require.NoError(t, err)
			require.Equal(t, tt.wantTotal, resp.Total)
		})
	}
}

func TestStoreSenderBatching(t *testing.T) {
	docs := make([][]byte, 0, 300)
	for i := range 300 {
		docs = append(docs, []byte(`{"timestamp":"2000-01-01T13:00:00Z","message":"doc `+itoa(i)+`"}`))
	}

	mapping := seq.TestMapping
	client, st := newTestStore(t, mapping)
	s := newStoreSenderWithClient(client, mapping)

	// two batches through the same sender reuse buffers
	require.NoError(t, s.Send(context.Background(), docs[:200]))
	require.NoError(t, s.Send(context.Background(), docs[200:]))
	st.WaitIdle()

	resp, err := s.client.Search(context.Background(), &storeapiclient.SearchRequest{
		Query:     "message:doc",
		From:      0,
		To:        1<<62 - 1,
		Size:      1000,
		WithTotal: true,
	}, grpcMaxCallOpts...)
	require.NoError(t, err)
	require.Equal(t, uint64(300), resp.Total)
}

func itoa(i int) string {
	return string([]byte{byte('0' + i/100), byte('0' + i/10%10), byte('0' + i%10)})
}

func TestNewSenderRequiresMapping(t *testing.T) {
	_, _, err := newSender(modeStore, "localhost:1", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--mapping")
}
