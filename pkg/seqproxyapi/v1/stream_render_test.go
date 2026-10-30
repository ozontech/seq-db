package seqproxyapi

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/query/encoding"
)

func TestRowSchemaRender(t *testing.T) {
	r := require.New(t)

	tsNs := time.Date(2025, 7, 8, 10, 19, 8, 742000000, time.UTC).UnixNano()

	test := func(typing []*Typing, cells [][]byte, expected string) {
		t.Helper()

		schema := newRowSchema(typing)
		raw, err := schema.renderRecord(cells)
		r.NoError(err)
		r.Equal(expected, string(raw))
	}

	t.Run("documents", func(t *testing.T) {
		test(
			[]*Typing{
				{Title: "id", Type: DataType_DATA_TYPE_SEQ_ID},
				{Title: "time", Type: DataType_DATA_TYPE_UINT64},
				{Title: "data", Type: DataType_DATA_TYPE_RAW_DOCUMENT},
			},
			[][]byte{
				[]byte("46e48be997010000-e70163d0fa7582e4"),
				encoding.Uint64ToBytes(uint64(tsNs)),
				[]byte(`{"message":"some_message","level":3}`),
			},
			`["46e48be997010000-e70163d0fa7582e4","2025-07-08T10:19:08.742Z",{"message":"some_message","level":3}]`,
		)
	})

	t.Run("aggregation bucket", func(t *testing.T) {
		test(
			[]*Typing{
				{Title: "token", Type: DataType_DATA_TYPE_STRING},
				{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
				{Title: "ts", Type: DataType_DATA_TYPE_UINT64},
				{Title: "quantiles", Type: DataType_DATA_TYPE_FLOAT64_ARRAY},
				{Title: "not_exists", Type: DataType_DATA_TYPE_UINT64},
			},
			[][]byte{
				[]byte("service-a"),
				encoding.Float64ToBytes(42.5),
				encoding.Uint64ToBytes(uint64(tsNs)),
				encoding.Float64ArrayToBytes(nil), // no quantiles
				encoding.Uint64ToBytes(7),
			},
			`["service-a",42.5,"2025-07-08T10:19:08.742Z",[],7]`,
		)
	})

	t.Run("aggregation bucket with quantiles", func(t *testing.T) {
		test(
			[]*Typing{
				{Title: "token", Type: DataType_DATA_TYPE_STRING},
				{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
				{Title: "ts", Type: DataType_DATA_TYPE_UINT64},
				{Title: "quantiles", Type: DataType_DATA_TYPE_FLOAT64_ARRAY},
				{Title: "not_exists", Type: DataType_DATA_TYPE_UINT64},
			},
			[][]byte{
				[]byte("service-a"),
				encoding.Float64ToBytes(42.5),
				encoding.Uint64ToBytes(uint64(tsNs)),
				encoding.Float64ArrayToBytes([]float64{5.5, 9.1}),
				encoding.Uint64ToBytes(0),
			},
			`["service-a",42.5,"2025-07-08T10:19:08.742Z",[5.5,9.1],0]`,
		)
	})

	t.Run("aggregation bucket with NaN and no ts", func(t *testing.T) {
		test(
			[]*Typing{
				{Title: "token", Type: DataType_DATA_TYPE_STRING},
				{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
				{Title: "ts", Type: DataType_DATA_TYPE_UINT64},
				{Title: "quantiles", Type: DataType_DATA_TYPE_FLOAT64_ARRAY},
				{Title: "not_exists", Type: DataType_DATA_TYPE_UINT64},
			},
			[][]byte{
				[]byte("service-a"),
				encoding.Float64ToBytes(math.NaN()),
				make([]byte, 8),
				encoding.Float64ArrayToBytes(nil),
				make([]byte, 8),
			},
			`["service-a","NaN","",[],0]`,
		)
	})

	t.Run("count without ts renders zero timestamp as empty string", func(t *testing.T) {
		// A flexible schema may omit ts, quantiles and not_exists entirely.
		test(
			[]*Typing{
				{Title: "token", Type: DataType_DATA_TYPE_STRING},
				{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
			},
			[][]byte{
				[]byte("service-a"),
				encoding.Float64ToBytes(3),
			},
			`["service-a",3]`,
		)
	})

	t.Run("uint64 not a timestamp renders as number", func(t *testing.T) {
		test(
			[]*Typing{{Title: "total", Type: DataType_DATA_TYPE_UINT64}},
			[][]byte{encoding.Uint64ToBytes(42)},
			`[42]`,
		)
	})
}

func TestRowSchemaRenderMismatch(t *testing.T) {
	r := require.New(t)

	schema := newRowSchema([]*Typing{
		{Title: "token", Type: DataType_DATA_TYPE_STRING},
		{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
	})

	_, err := schema.renderRecord([][]byte{[]byte("service-a")})
	r.Error(err)

	// The data message renders record as raw base64 cells instead of failing.
	raw, err := json.Marshal(&renderedData{
		schema: schema,
		batch:  &RecordsBatch{Records: []*Record{{RawData: [][]byte{[]byte("service-a")}}}},
	})
	r.NoError(err)
	r.Equal(
		`{"data":{"batch":{"records":[["c2VydmljZS1h"]]}}}`,
		string(raw),
	)
}

func TestStreamSearchRewriter(t *testing.T) {
	r := require.New(t)

	rewrite := NewStreamSearchRewriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Messages of other RPCs pass through untouched.
	unary := &StatusResponse{}
	out, err := rewrite(ctx, unary)
	r.NoError(err)
	r.Equal(unary, out)

	// The header carries the schema and passes through unchanged.
	header := &StreamSearchResponse{ResponseType: &StreamSearchResponse_Header{Header: &ResponseHeader{
		Typing: []*Typing{
			{Title: "token", Type: DataType_DATA_TYPE_STRING},
			{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
		},
	}}}
	out, err = rewrite(ctx, header)
	r.NoError(err)
	r.Equal(header, out)

	// A data message of the same stream is rewritten into a rendered batch.
	data := &StreamSearchResponse{ResponseType: &StreamSearchResponse_Data{Data: &ResponseData{
		Batch: &RecordsBatch{Records: []*Record{
			{RawData: [][]byte{[]byte("service-a"), encoding.Float64ToBytes(3)}},
			{RawData: [][]byte{[]byte("service-b"), encoding.Float64ToBytes(4.5)}},
		}},
	}}}
	out, err = rewrite(ctx, data)
	r.NoError(err)

	raw, err := json.Marshal(map[string]any{"result": out})
	r.NoError(err)
	r.Equal(
		`{"result":{"data":{"batch":{"records":[["service-a",3],["service-b",4.5]]}}}}`,
		string(raw),
	)

	// The summary closes the stream and drops the captured schema.
	summary := &StreamSearchResponse{ResponseType: &StreamSearchResponse_Summary{Summary: &ResponseSummary{}}}
	out, err = rewrite(ctx, summary)
	r.NoError(err)
	r.Equal(summary, out)

	// Without a captured schema a data message passes through and renders its records as raw base64.
	out, err = rewrite(ctx, data)
	r.NoError(err)
	raw, err = json.Marshal(map[string]any{"result": out})
	r.NoError(err)
	r.Contains(string(raw), "c2VydmljZS1h") // == base64("service-a")
}

func TestStreamSearchRewriterSchemaCleanup(t *testing.T) {
	r := require.New(t)

	newHeader := func(_ context.Context) *StreamSearchResponse {
		return &StreamSearchResponse{ResponseType: &StreamSearchResponse_Header{
			Header: &ResponseHeader{Typing: []*Typing{
				{Title: "value", Type: DataType_DATA_TYPE_FLOAT64},
			}},
		}}
	}

	rewrite := NewStreamSearchRewriter()
	ctx, cancel := context.WithCancel(context.Background())

	_, err := rewrite(ctx, newHeader(ctx))
	r.NoError(err)

	// Aborting the stream (context cancellation) drops the captured schema on
	// the next stream's header, so a late data message is no longer rewritten.
	cancel()
	_, err = rewrite(context.Background(), newHeader(context.Background()))
	r.NoError(err)

	out, err := rewrite(ctx, &StreamSearchResponse{ResponseType: &StreamSearchResponse_Data{
		Data: &ResponseData{Batch: &RecordsBatch{Records: []*Record{}}}},
	})
	r.NoError(err)
	r.False(isRenderedData(out))
}

func isRenderedData(v any) bool {
	_, ok := v.(*renderedData)
	return ok
}
