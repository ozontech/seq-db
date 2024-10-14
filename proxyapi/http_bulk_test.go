package proxyapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
)

func Test_writeBulkResponse(t *testing.T) {
	w := bytes.NewBuffer(nil)
	writeBulkResponse(w, time.Second, 3)
	require.True(t, json.Valid(w.Bytes()))
}

type FakeBulkProcessor struct {
	Count int
}

func (s *FakeBulkProcessor) ProcessDocuments(_ context.Context, _ time.Time, cb func() ([]byte, error)) (int, error) {
	for {
		doc, err := cb()
		if err != nil {
			return s.Count, err
		}

		if len(doc) == 0 {
			return s.Count, nil
		}
		s.Count++
	}
}

func BenchmarkESBulk(b *testing.B) {
	const docsToLoad = 1000
	const payload = `{"create":{"_index":"test","_id":"1"}}` + "\n" +
		`{"level":"info","ts":"2024-07-02T11:35:19.076Z","message":"last fraction details","fraction":"sealed fraction name=seq-db-01J1FQ1W09JP1YC1E43PFT2CCQ, creation time=2024-06-28 15:22:35.401, from=2024-06-28 15:22:25.49, to=2024-06-28 15:23:08.893, raw docs=1.3 GB, disk docs=261.6 MB"}`
	const totalSize = len(payload) * docsToLoad

	buf := make([]byte, 0, totalSize)
	for i := 0; i < docsToLoad; i++ {
		buf = append(buf, []byte(payload)...)
		buf = append(buf, '\n')
	}

	proc := &FakeBulkProcessor{}
	handler := NewBulkHandler(proc, consts.KB*512)

	request := http.Request{}
	reqBodyBuf := bytes.NewReader(buf)
	request.Body = io.NopCloser(reqBodyBuf)

	response := httptest.NewRecorder()
	response.Body = bytes.NewBuffer(make([]byte, 0, totalSize))

	b.SetBytes(int64(totalSize))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reqBodyBuf.Seek(0, io.SeekStart)
		response.Body.Reset()
		proc.Count = 0

		handler.ServeHTTP(response, &request)

		if proc.Count != docsToLoad {
			b.Fatalf("expected %d docs, got %d", docsToLoad*b.N, proc.Count)
		}
	}
}

func TestScanLines(t *testing.T) {
	t.Parallel()

	const maxLineSize = 32
	r := require.New(t)

	test := func(in string, wantDocs int, wantErr error) {
		t.Helper()

		proc := &FakeBulkProcessor{}
		handler := NewBulkHandler(proc, maxLineSize)

		total, err := handler.handleESBulkRequest(context.Background(), strings.NewReader(in))

		if wantErr != nil {
			r.ErrorIs(err, wantErr)
		} else {
			r.NoError(err)
		}
		r.Equal(wantDocs, total)
		r.Equal(wantDocs, proc.Count)
	}

	test(`{"create":{}}
{"level": "info"}`, 1, nil)

	test(`{"create":{}}
{"level": "info"}
{"create":{}}
{"level": "info"}`, 2, nil)

	// Trailing newline.
	test(`{"create":{}}
{"level": "info"}
`, 1, nil)

	// Skip empty lines.
	test(`

{"create":{}}
{"level": "info"}

`, 1, nil)

	// Do not skip empty line after action command.
	test(`

{"create":{}}

{"level": "info"}

`, 0, errWrongProtocol)

	test(`{"invalid action"}`, 0, errWrongProtocol)

	test(`{"create":{}}`+"\n"+strings.Repeat("a", maxLineSize+1), 0, nil)
}
