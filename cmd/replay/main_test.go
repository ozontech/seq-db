package main

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/storage"
)

func TestReadDocs(t *testing.T) {
	var rawOverflow []byte
	rawOverflow = binary.LittleEndian.AppendUint32(rawOverflow, 100)
	rawOverflow = append(rawOverflow, []byte("short")...)

	tests := []struct {
		name      string
		blocks    [][]byte
		want      []string
		wantErr   string
		stopsOn   string
		stopError error
	}{
		{
			name:   "ok single block",
			blocks: [][]byte{packDocBlock(t, "doc1", "doc2")},
			want:   []string{"doc1", "doc2"},
		},
		{
			name: "ok multiple blocks",
			blocks: [][]byte{
				packDocBlock(t, "a", "b"),
				packDocBlock(t, "c"),
				packDocBlock(t),
			},
			want: []string{"a", "b", "c"},
		},
		{
			name: "empty file",
			want: nil,
		},
		{
			name:      "truncated block header",
			blocks:    [][]byte{make([]byte, storage.DocBlockHeaderLen-1)},
			wantErr:   "truncated doc block header at offset 0",
			stopError: nil,
		},
		{
			name:    "truncated doc length",
			blocks:  [][]byte{packRawDocBlock(t, []byte{1, 2})},
			wantErr: "truncated doc length",
		},
		{
			name:    "doc length exceeds block size",
			blocks:  [][]byte{packRawDocBlock(t, rawOverflow)},
			wantErr: "exceeds block size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeDocsFile(t, tt.blocks...)

			docs, err := openDocs(path)
			require.NoError(t, err)
			defer docs.Close()

			var got []string
			for {
				doc, err := docs.next()
				if tt.wantErr != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tt.wantErr)
					return
				}
				require.NoError(t, err)
				if doc == nil {
					break
				}
				got = append(got, string(doc))
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDocsReaderStopOnError(t *testing.T) {
	path := writeDocsFile(t, packDocBlock(t, "a", "b"))

	docs, err := openDocs(path)
	require.NoError(t, err)
	defer docs.Close()

	doc, err := docs.next()
	require.NoError(t, err)
	require.Equal(t, "a", string(doc))
}

func packDocBlock(t *testing.T, docs ...string) []byte {
	t.Helper()

	var raw []byte
	for _, doc := range docs {
		raw = binary.LittleEndian.AppendUint32(raw, uint32(len(doc)))
		raw = append(raw, doc...)
	}
	return packRawDocBlock(t, raw)
}

func packRawDocBlock(t *testing.T, raw []byte) []byte {
	t.Helper()

	block := make(storage.DocBlock, storage.DocBlockHeaderLen)
	return storage.CompressDocBlock(raw, block, 1)
}

func writeDocsFile(t *testing.T, blocks ...[]byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "testfrac.docs")
	var data []byte
	for _, b := range blocks {
		data = append(data, b...)
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func TestProxySender(t *testing.T) {
	docs := [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)}

	tests := []struct {
		name      string
		status    func(attempt int) int
		canceled  bool
		wantErr   []string
		wantCalls int64
	}{
		{
			name:      "ok first attempt",
			status:    statusOK,
			wantCalls: 1,
		},
		{
			name:      "retry on 429 then success",
			status:    status429Twice,
			wantCalls: 3,
		},
		{
			name:      "no retry on 400",
			status:    status400,
			wantErr:   []string{"status 400"},
			wantCalls: 1,
		},
		{
			name:      "gives up after max attempts",
			status:    status500,
			wantErr:   []string{"giving up after 10 attempts"},
			wantCalls: int64(maxBulkTries),
		},
		{
			name:      "context canceled between retries",
			status:    status429Always,
			canceled:  true,
			wantErr:   []string{"context canceled"},
			wantCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, calls := newTestServer(t, tt.status)

			ctx := context.Background()
			if tt.canceled {
				newCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = newCtx
			}

			s := newTestSender(srv.URL)
			err := s.Send(ctx, docs)

			if tt.wantErr != nil {
				for _, substr := range tt.wantErr {
					require.ErrorContains(t, err, substr)
				}
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantCalls, calls.Load())
		})
	}
}

func statusOK(int) int        { return http.StatusOK }
func status400(int) int       { return http.StatusBadRequest }
func status500(int) int       { return http.StatusInternalServerError }
func status429Always(int) int { return http.StatusTooManyRequests }

func status429Twice(n int) int {
	if n < 3 {
		return http.StatusTooManyRequests
	}
	return http.StatusOK
}

func newTestServer(t *testing.T, status func(attempt int) int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		code := status(int(n))
		if code != http.StatusOK {
			w.WriteHeader(code)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func newTestSender(url string) *proxySender {
	s := newProxySender(url)
	s.backoff = instantBackoff
	return s
}

func instantBackoff(int) time.Duration { return time.Millisecond }
