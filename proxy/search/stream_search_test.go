package search

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func TestStreamSearchIteratorReadsAllRecords(t *testing.T) {
	id1 := seq.SimpleID(1)
	id2 := seq.SimpleID(2)
	id3 := seq.SimpleID(3)

	it, _ := newTestIterator(t,
		dataMsg(testRecord(id1, "doc1"), testRecord(id2, "doc2")),
		dataMsg(testRecord(id3, "doc3")),
	)

	got := collectAll(it)
	require.Len(t, got, 3)
	assert.Equal(t, id1, got[0].Vals[0].Decoded().(seq.ID))
	assert.Equal(t, "doc1", string(got[0].Vals[1].RawData()))
	assert.Equal(t, id2, got[1].Vals[0].Decoded().(seq.ID))
	assert.Equal(t, "doc2", string(got[1].Vals[1].RawData()))
	assert.Equal(t, id3, got[2].Vals[0].Decoded().(seq.ID))
	assert.Equal(t, "doc3", string(got[2].Vals[1].RawData()))
}

func TestStreamSearchIteratorEmptyStream(t *testing.T) {
	// EOF right after the header: an empty result, not an error.
	it, stream := newTestIterator(t)
	stream.setRecvErr(io.EOF)

	assert.Empty(t, collectAll(it))
	assert.Zero(t, it.Finalize().Total)
	assert.False(t, stream.isCanceled(), "Finalize must not cancel the stream context")
}

func TestStreamSearchIteratorSummaryAfterExhausted(t *testing.T) {
	// Summary sent after the last data batch; it is captured via Next.
	it, stream := newTestIterator(t,
		dataMsg(testRecord(seq.SimpleID(1), "doc1")),
		summaryMsg(42, storeapi.SearchErrorCode_NO_ERROR),
	)

	assert.Len(t, collectAll(it), 1)
	summary := it.Finalize()

	assert.Nil(t, summary.Err)
	assert.Equal(t, uint64(42), summary.Total)
	assert.False(t, stream.isCanceled())
	require.Len(t, stream.sent, 1)
	assert.Equal(t, storeapi.ControlAction_FINALIZE, stream.sent[0].GetControl().GetAction())
}

func TestStreamSearchIteratorFinalizeMidStream(t *testing.T) {
	it, stream := newTestIterator(t,
		dataMsg(testRecord(seq.SimpleID(1), "doc1")),
		dataMsg(testRecord(seq.SimpleID(2), "doc2")),
		summaryMsg(100, storeapi.SearchErrorCode_NO_ERROR),
	)

	// Read only the first record, then finalize.
	assert.NotNil(t, it.Next())
	summary := it.Finalize()

	assert.Nil(t, summary.Err)
	assert.Equal(t, uint64(100), summary.Total)
	assert.False(t, stream.isCanceled(), "Finalize must drain, not cancel")
}

func TestStreamSearchIteratorSummaryWithError(t *testing.T) {
	it, _ := newTestIterator(t, summaryMsg(0, storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT))

	// Fail-fast summary arrives before any data: detected on the open-stream
	// phase via the prefetch in NewStreamSearchIterator.
	assert.Empty(t, collectAll(it))
	summary := it.Finalize()

	require.ErrorIs(t, summary.Err, consts.ErrTooManyFractionsHit)
}

func TestStreamSearchIteratorClose(t *testing.T) {
	it, stream := newTestIterator(t,
		dataMsg(testRecord(seq.SimpleID(1), "doc1")),
		dataMsg(testRecord(seq.SimpleID(2), "doc2")),
	)
	// The store keeps producing: without cancellation Recv would block.
	stream.blockAtEnd = true

	assert.NotNil(t, it.Next())

	it.Close()
	assert.True(t, stream.isCanceled(), "Close must cancel the stream context")

	// Close is idempotent and safe to call concurrently with Next.
	it.Close()
	assert.NotNil(t, it.Next())

	require.Empty(t, stream.sent, "Close must not send control messages")
}

func TestStreamSearchIteratorNextAfterRecvError(t *testing.T) {
	it, stream := newTestIterator(t, dataMsg(testRecord(seq.SimpleID(1), "doc1")))

	// Read the first record, then the stream dies with a non-EOF error.
	assert.NotNil(t, it.Next())
	stream.setRecvErr(errors.New("connection reset"))

	assert.Nil(t, it.Next())
	summary := it.Finalize()
	require.Error(t, summary.Err)
	assert.Contains(t, summary.Err.Error(), "connection reset")
}

func TestStreamSearchIteratorUnexpectedHeader(t *testing.T) {
	it, stream := newTestIterator(t)
	stream.push(&storeapi.StreamSearchResponse{
		ResponseType: &storeapi.StreamSearchResponse_Header{
			Header: &storeapi.ResponseHeader{Typing: testTyping()},
		},
	})

	assert.Nil(t, it.Next())
	summary := it.Finalize()
	require.Error(t, summary.Err)
	assert.Contains(t, summary.Err.Error(), "unexpected header")
	assert.False(t, stream.isCanceled())
}

func TestNewStreamSearchIteratorPrefetchedErrorSummary(t *testing.T) {
	// The summary arrives immediately after the header and must be detected
	// on the open-stream phase, before any Next call.
	stream := newFakeStream()
	stream.push(summaryMsg(0, storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA))
	it, err := NewStreamSearchIterator(
		querytracer.New(false, "test"),
		&storeapi.ResponseHeader{Typing: testTyping()},
		stream,
		stream.markCanceled,
	)
	require.NoError(t, err)

	assert.Nil(t, it.Next())
	summary := it.Finalize()
	require.ErrorIs(t, summary.Err, consts.ErrIngestorQueryWantsOldData)
	assert.False(t, stream.isCanceled(), "cancel must not fire before Close is called")
}

func TestNewStreamSearchIteratorEOFOnPrefetch(t *testing.T) {
	// EOF on prefetch: no data and no summary, an empty result.
	stream := newFakeStream()
	stream.setRecvErr(io.EOF)
	it, err := NewStreamSearchIterator(
		querytracer.New(false, "test"),
		&storeapi.ResponseHeader{Typing: testTyping()},
		stream,
		stream.markCanceled,
	)
	require.NoError(t, err)

	assert.Nil(t, it.Next())
	summary := it.Finalize()
	assert.Nil(t, summary.Err)
	assert.False(t, stream.isCanceled())
}

func testTyping() []*storeapi.Typing {
	return []*storeapi.Typing{
		{Title: "id", Type: storeapi.DataType_SEQ_ID},
		{Title: "data", Type: storeapi.DataType_RAW_DOCUMENT},
	}
}

func testRecord(id seq.ID, data string) *storeapi.Record {
	return &storeapi.Record{RawData: [][]byte{
		encoding.SeqIDToBytes(id),
		[]byte(data),
	}}
}

func dataMsg(records ...*storeapi.Record) *storeapi.StreamSearchResponse {
	return &storeapi.StreamSearchResponse{
		ResponseType: &storeapi.StreamSearchResponse_Data{
			Data: &storeapi.ResponseData{Batch: &storeapi.RecordsBatch{Records: records}},
		},
	}
}

func summaryMsg(total uint64, errCode storeapi.SearchErrorCode) *storeapi.StreamSearchResponse {
	s := &storeapi.ResponseSummary{Total: total}
	if errCode != storeapi.SearchErrorCode_NO_ERROR {
		s.Error = &storeapi.Error{Code: errCode}
	}
	return &storeapi.StreamSearchResponse{
		ResponseType: &storeapi.StreamSearchResponse_Summary{Summary: s},
	}
}

func collectAll(p query.RecordProducer) []*query.Record {
	records := make([]*query.Record, 0)
	for r := p.Next(); r != nil; r = p.Next() {
		records = append(records, r)
	}
	return records
}

func newTestIterator(t *testing.T, msgs ...*storeapi.StreamSearchResponse) (*StreamSearchIterator, *fakeStream) {
	t.Helper()

	stream := newFakeStream()
	stream.msgs = msgs
	it, err := NewStreamSearchIterator(
		querytracer.New(false, "test"),
		&storeapi.ResponseHeader{Typing: testTyping()},
		stream,
		stream.markCanceled,
	)
	require.NoError(t, err)
	return it, stream
}

type fakeStream struct {
	msgs       []*storeapi.StreamSearchResponse
	sent       []*storeapi.StreamSearchRequest
	sendErr    error
	recvErr    error
	blockAtEnd bool

	cancelOnce sync.Once
	canceledCh chan struct{}
}

func newFakeStream() *fakeStream {
	return &fakeStream{canceledCh: make(chan struct{})}
}

func (f *fakeStream) push(msg *storeapi.StreamSearchResponse) {
	f.msgs = append(f.msgs, msg)
}

func (f *fakeStream) setRecvErr(err error) {
	f.recvErr = err
}

func (f *fakeStream) markCanceled() {
	f.cancelOnce.Do(func() { close(f.canceledCh) })
}

func (f *fakeStream) isCanceled() bool {
	select {
	case <-f.canceledCh:
		return true
	default:
		return false
	}
}

func (f *fakeStream) Recv() (*storeapi.StreamSearchResponse, error) {
	if len(f.msgs) == 0 {
		if f.recvErr != nil {
			return nil, f.recvErr
		}
		if f.blockAtEnd {
			// Block until the stream context is canceled, mimicking a live RPC that keeps producing.
			<-f.canceledCh
			return nil, context.Canceled
		}
		return nil, io.EOF
	}
	msg := f.msgs[0]
	f.msgs = f.msgs[1:]
	return msg, nil
}

func (f *fakeStream) Send(req *storeapi.StreamSearchRequest) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, req)
	return nil
}

func (f *fakeStream) CloseSend() error { return nil }

func (f *fakeStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeStream) Trailer() metadata.MD         { return nil }
func (f *fakeStream) Close() error                 { return nil }
func (f *fakeStream) Context() context.Context     { return context.Background() }
func (f *fakeStream) SendMsg(any) error            { return nil }
func (f *fakeStream) RecvMsg(any) error            { return nil }
