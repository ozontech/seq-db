package setup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
)

func GenBuffer(docs []string) *bytes.Buffer {
	b := bytes.NewBuffer(nil)
	for _, doc := range docs {
		_, _ = b.WriteString(`{"index":"seq-db"}` + "\n")
		_, _ = b.WriteString(doc)
		_, _ = b.WriteString("\n")
	}
	return b
}

func GenBufferBytesReuse(docs [][]byte, b *bytes.Buffer) *bytes.Buffer {
	for _, doc := range docs {
		_, _ = b.WriteString(`{"index":"seq-db"}` + "\n")
		_, _ = b.Write(doc)
		_, _ = b.WriteString("\n")
	}
	return b
}

func GenBufferBytes(docs [][]byte) *bytes.Buffer {
	return GenBufferBytesReuse(docs, bytes.NewBuffer(nil))
}

func BulkBuffer(t *testing.T, addr string, b *bytes.Buffer) {
	r, err := http.Post(addr, "", b)
	require.NoError(t, err, "should be no errors")
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	if !assert.Equalf(t, http.StatusOK, r.StatusCode, "wrong response with status=%v: %s", r.Status, string(body)) {
		body, err := io.ReadAll(r.Body)
		if err == nil {
			logger.Error(string(body))
		} else {
			logger.Error("can't read body", zap.Error(err))
		}
		t.FailNow()
	}
}

func Bulk(t *testing.T, addr string, docs []string) {
	b := GenBuffer(docs)
	BulkBuffer(t, addr, b)
}

func BulkBytes(t *testing.T, addr string, docs [][]byte) {
	b := GenBufferBytes(docs)
	BulkBuffer(t, addr, b)
}

func SearchHTTP(t *testing.T, addr string, request *seqproxyapi.SearchRequest) *seqproxyapi.SearchResponse {
	req, err := protojson.Marshal(request)
	require.NoError(t, err)

	r, err := http.Post(addr, "application/json", bytes.NewReader(req))
	require.NoError(t, err, "should be no errors")
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, r.StatusCode)

	searchResponse := &seqproxyapi.SearchResponse{}
	fmt.Println(string(body))
	require.NoError(t, json.Unmarshal(body, searchResponse))
	require.Equal(t, http.StatusOK, r.StatusCode, "wrong http status")
	return searchResponse
}

func FetchHTTP(t *testing.T, addr string, ids []string) []*seqproxyapi.Document {
	request := &seqproxyapi.FetchRequest{
		Ids: ids,
	}
	req, err := protojson.Marshal(request)
	require.NoError(t, err)

	r, err := http.Post(addr, "application/json", bytes.NewReader(req))
	require.NoError(t, err, "should be no errors")
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	require.Equalf(t, http.StatusOK, r.StatusCode, "%s", body)

	var fetchResponse []*seqproxyapi.Document
	for i := 0; i < len(ids); i++ {
		pos := bytes.IndexByte(body, '\n')
		require.True(t, pos > 0)

		result := struct {
			Result *seqproxyapi.Document `json:"result"`
		}{}
		require.NoError(t, json.Unmarshal(body[:pos], &result))
		body = body[pos+1:]

		fetchResponse = append(fetchResponse, result.Result)
	}

	return fetchResponse
}
