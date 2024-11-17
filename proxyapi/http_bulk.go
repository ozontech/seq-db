package proxyapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/klauspost/compress/gzip"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/tracing"
)

var (
	bulkDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "duration_seconds",
		Buckets:   metric.SecondsBuckets,
	})
	bulkAPIError = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "api_errors_total",
		Help:      "",
	})
	bulkReadDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "read_duration_seconds",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	})
)

type DocumentsProcessor interface {
	ProcessDocuments(ctx context.Context, requestTime time.Time, readNext func() ([]byte, error)) (int, error)
}

type BulkHandler struct {
	proc            DocumentsProcessor
	maxDocumentSize int
}

func NewBulkHandler(proc DocumentsProcessor, maxDocumentSize int) *BulkHandler {
	return &BulkHandler{
		proc:            proc,
		maxDocumentSize: maxDocumentSize,
	}
}

type measuredReader struct {
	metric   prometheus.Histogram
	reader   io.Reader
	total    time.Duration
	reported bool
}

func newMeasuredReader(reader io.Reader, histogram prometheus.Histogram) *measuredReader {
	return &measuredReader{
		reader:   reader,
		metric:   histogram,
		reported: false,
		total:    0,
	}
}

func (mr *measuredReader) Read(p []byte) (int, error) {
	t := time.Now()
	n, err := mr.reader.Read(p)
	mr.total += time.Since(t)
	if err != nil {
		if errors.Is(err, io.EOF) && !mr.reported {
			// End of the stream, report the metric.
			mr.metric.Observe(mr.total.Seconds())
			// Do not report the metric again.
			// It may be reported in the next call to Read.
			mr.reported = true
		}
		return n, err
	}
	return n, nil
}

func (h *BulkHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.HTTPSpan(r, "search_proxy.ServeBulk", 0.01)
	defer span.End()

	t := time.Now()

	body := io.Reader(newMeasuredReader(r.Body, bulkReadDurationSeconds))
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := acquireGzipReader(body)
		if err != nil {
			// Body is not gzipped
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer putGzipReader(gz)
		body = io.Reader(gz)
	}

	total, err := h.handleESBulkRequest(ctx, body)
	if err != nil {
		bulkAPIError.Inc()
		logger.Error("ingest error", zap.Error(err))

		statusCode := http.StatusInternalServerError
		if errors.Is(err, bulk.ErrTooManyInflightBulks) {
			statusCode = http.StatusTooManyRequests
		}
		if errors.Is(err, errWrongProtocol) {
			statusCode = http.StatusBadRequest
			logger.Error("wrong bulk protocol")
		}
		http.Error(w, err.Error(), statusCode)
		return
	}

	took := time.Since(t)
	bulkDurationSeconds.Observe(took.Seconds())

	writeBulkResponse(w, took, total)
}

var (
	errWrongProtocol = errors.New("wrong protocol")
)

func (h *BulkHandler) handleESBulkRequest(ctx context.Context, body io.Reader) (int, error) {
	reader := acquireESBulkDocReader(body, h.maxDocumentSize)
	defer releaseESBulkDocReader(reader)

	return h.proc.ProcessDocuments(ctx, time.Now(), reader.ReadDoc)
}

type esBulkDocReader struct {
	r               *bufio.Reader
	actionLinesRead int
}

var esBulkDocReaderPool = sync.Pool{
	New: func() any {
		return new(esBulkDocReader)
	},
}

func acquireESBulkDocReader(reader io.Reader, maxDocumentSize int) *esBulkDocReader {
	r := esBulkDocReaderPool.Get().(*esBulkDocReader)
	r.actionLinesRead = 0
	if r.r == nil {
		r.r = bufio.NewReaderSize(reader, maxDocumentSize)
	} else {
		r.r.Reset(reader)
	}
	return r
}

func releaseESBulkDocReader(r *esBulkDocReader) {
	esBulkDocReaderPool.Put(r)
}

func (r *esBulkDocReader) ReadDoc() ([]byte, error) {
	var doc []byte
	for {
		err := r.skipActionLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, fmt.Errorf("scanning action line: %w", err)
		}

		var sizeExceeded bool
		doc, sizeExceeded, err = r.readDoc()
		if err != nil {
			return nil, fmt.Errorf("reading document: %w", err)
		}
		if !sizeExceeded {
			// We found the document that is smaller than maxDocumentSize.
			break
		}
		// Document size is too large, skip it.
	}

	if len(doc) == 0 {
		return nil, fmt.Errorf("%w: empty document after action line", errWrongProtocol)
	}

	// doc is part of the reader’s buffer, and we can corrupt it after appending.
	// Therefore, we limit the capacity to ensure that it is safe to allocate a new memory area
	// when append is called and not corrupt the buffer.
	doc = doc[:len(doc):len(doc)]

	return doc, nil
}

// skipActionLine skips bulk action line.
// For example, `{"create":{...}}`, `{"index":{...}}`.
func (r *esBulkDocReader) skipActionLine() error {
	var action []byte
	for len(action) == 0 {
		line, isPrefix, err := r.r.ReadLine()
		if err != nil {
			return err
		}
		if isPrefix {
			return fmt.Errorf("%w: action line is too long", errWrongProtocol)
		}
		action = line
	}

	actionLineStr := unsafe.String(unsafe.SliceData(action), len(action))
	// Check first 5 action lines to ensure that the protocol is correct.
	const actionLinesToCheck = 5
	if r.actionLinesRead < actionLinesToCheck && !strings.Contains(actionLineStr, `"create"`) && !strings.Contains(actionLineStr, `"index"`) {
		return fmt.Errorf("%w: unknown action line=%q", errWrongProtocol, actionLineStr)
	}
	r.actionLinesRead++

	return nil
}

func (r *esBulkDocReader) readDoc() (doc []byte, sizeExceeded bool, _ error) {
	doc, isPrefix, err := r.r.ReadLine()
	if err != nil {
		return nil, false, err
	}
	if !isPrefix {
		return doc, false, nil
	}

	// Clone first part of the document.
	docStr := string(doc[:min(256, len(doc))])

	// If this is a line that exceeds maxDocumentSize, skip it.
	docLen := len(doc)
	for isPrefix {
		doc, isPrefix, err = r.r.ReadLine()
		if err != nil {
			return nil, true, err
		}
		docLen += len(doc)
	}

	logger.Error("skipping document due to max document size exceeded, check --max-document-size flag for more details",
		zap.String("prefix", docStr), zap.Int("doc_len", docLen))

	return nil, true, nil
}

func writeBulkResponse(w io.Writer, took time.Duration, total int) {
	const itemCreated = `{"create":{"status":201}}`
	const maxPrefixLen = 256
	bufferSize := maxPrefixLen + len(itemCreated)*total
	response := bytespool.AcquireWriterSize(w, bufferSize)
	defer bytespool.FlushReleaseWriter(response)

	_, _ = response.WriteString(`{"took":`)
	response.Buf.B = strconv.AppendInt(response.Buf.B, took.Milliseconds(), 10)
	_, _ = response.WriteString(`,"errors":false,"items":[`)
	for i := 0; i < total; i++ {
		if i != 0 {
			_, _ = response.WriteString(`,`)
		}
		_, _ = response.WriteString(itemCreated)
	}
	_, _ = response.WriteString(`]}`)
}

var gzipReaderPool sync.Pool

func acquireGzipReader(r io.Reader) (*gzip.Reader, error) {
	anyReader := gzipReaderPool.Get()
	if anyReader == nil {
		return gzip.NewReader(r)
	}
	gzReader := anyReader.(*gzip.Reader)
	err := gzReader.Reset(r)
	return gzReader, err
}

func putGzipReader(reader *gzip.Reader) {
	_ = reader.Close()
	gzipReaderPool.Put(reader)
}
