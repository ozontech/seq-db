package bulk

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ozontech/seq-db/seq"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/network/circuitbreaker"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/tokenizer"
)

var (
	inflightBulks = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "in_flight_queries_total",
		Help:      "",
	})

	bulkParseDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "parse_duration_seconds",
		Help:      "",
		Buckets:   metric.SecondsBuckets,
	})
)

type MappingProvider interface {
	GetMapping() seq.Mapping
	GetRawMapping() *seq.RawMapping
}

type IngestorConfig struct {
	HotStores   *stores.Stores
	WriteStores *stores.Stores

	BulkCircuit circuitbreaker.Config

	MaxInflightBulks       int
	AllowedTimeDrift       time.Duration
	FutureAllowedTimeDrift time.Duration

	MappingProvider MappingProvider

	MaxTokenSize         int
	CaseSensitive        bool
	PartialFieldIndexing bool

	DocsZSTDCompressLevel  int
	MetasZSTDCompressLevel int

	MaxDocumentSize int
}

type StorageClient interface {
	StoreDocuments(ctx context.Context, count int, docs, metas []byte) error
}

type Ingestor struct {
	config IngestorConfig

	client StorageClient

	tokenizers map[seq.TokenizerType]tokenizer.Tokenizer
	procPool   *sync.Pool

	inflight *atomic.Int64
	bulks    *atomic.Int64
	docs     *atomic.Int64
	took     *atomic.Int64

	stopped *atomic.Bool
}

func NewIngestor(c IngestorConfig, client StorageClient) *Ingestor {
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(c.MaxTokenSize, c.CaseSensitive, c.PartialFieldIndexing, consts.MaxTextFieldValueLength),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(c.MaxTokenSize, c.CaseSensitive, c.PartialFieldIndexing),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(c.MaxTokenSize, c.CaseSensitive, c.PartialFieldIndexing),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}

	i := &Ingestor{
		config:     c,
		client:     client,
		tokenizers: tokenizers,
		inflight:   &atomic.Int64{},
		bulks:      &atomic.Int64{},
		docs:       &atomic.Int64{},
		took:       &atomic.Int64{},
		stopped:    &atomic.Bool{},
		procPool:   &sync.Pool{},
	}

	go i.stats()

	return i
}

func (i *Ingestor) stats() {
	for {
		if i.stopped.Load() {
			return
		}
		time.Sleep(consts.ProxyBulkStatsInterval)
		if i.bulks.Load() > 0 {
			logger.Info("bulks written",
				zap.Int64("count", i.bulks.Swap(0)),
				zap.Int64("docs", i.docs.Swap(0)),
				zap.Int64("took_ms", i.took.Swap(0)),
				zap.Int64("inflight_bulks", i.inflight.Load()),
			)
		}
	}
}

func (i *Ingestor) Stop() {
	if i.stopped.Swap(true) {
		// Already stopped.
		return
	}
}

var ErrTooManyInflightBulks = errors.New("too many inflight bulks, dropping")

var (
	rateLimitedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Name:      "rate_limited_total",
		Help:      "Count of rate limited requests",
	})

	docsWritten = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "docs_written",
		Help:      "",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})

	notAnObjectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "not_an_object_errors_total",
		Help:      "Number of ingestion errors due to incorrect document type",
	})
)

func (i *Ingestor) ProcessDocuments(ctx context.Context, requestTime time.Time, readNext func() ([]byte, error)) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, consts.BulkTimeout)
	defer cancel()

	inflightBulks.Inc()
	defer inflightBulks.Dec()

	x := i.inflight.Add(1)
	defer i.inflight.Add(-1)

	if int(x) > i.config.MaxInflightBulks {
		rateLimitedTotal.Inc()
		logger.Error(ErrTooManyInflightBulks.Error(),
			zap.Int64("cur", x),
			zap.Int("limit", i.config.MaxInflightBulks),
		)
		return 0, ErrTooManyInflightBulks
	}

	t := time.Now()

	compressor := frac.GetDocsMetasCompressor(i.config.DocsZSTDCompressLevel, i.config.MetasZSTDCompressLevel)
	defer frac.PutDocMetasCompressor(compressor)

	total, err := i.processDocsToCompressor(compressor, requestTime, readNext)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		logger.Warn("bulk empty request, skipping")
		return 0, nil
	}

	docs, metas := compressor.DocsMetas()

	metric.IngestorBulkDocProvideDurationSeconds.Observe(time.Since(t).Seconds())

	t = time.Now()
	if err := i.client.StoreDocuments(ctx, total, docs, metas); err != nil {
		return 0, err
	}
	i.bulks.Add(1)
	i.docs.Add(int64(total))
	docsWritten.Observe(float64(total))
	i.took.Add(time.Since(t).Milliseconds())

	return total, nil
}

var (
	binaryDocsPool = sync.Pool{
		New: func() any {
			return new(bytespool.Buffer)
		},
	}
	binaryMetasPool = sync.Pool{
		New: func() any {
			return new(bytespool.Buffer)
		},
	}
)

func (i *Ingestor) processDocsToCompressor(
	compressor *frac.DocsMetasCompressor,
	requestTime time.Time,
	readNext func() ([]byte, error),
) (int, error) {
	parseDuration := time.Duration(0)

	proc := i.getProcessor()
	defer i.putProcessor(proc)

	binaryDocs := binaryDocsPool.Get().(*bytespool.Buffer)
	defer binaryDocsPool.Put(binaryDocs)
	binaryDocs.Reset()
	binaryMetas := binaryMetasPool.Get().(*bytespool.Buffer)
	defer binaryMetasPool.Put(binaryMetas)
	binaryMetas.Reset()

	total := 0
	for {
		originalDoc, err := readNext()
		if err != nil {
			return total, fmt.Errorf("reading next document: %s", err)
		}
		if originalDoc == nil {
			break
		}
		parseStart := time.Now()
		doc, metas, err := proc.Process(originalDoc, requestTime)
		if err != nil {
			if errors.Is(err, errNotAnObject) {
				logger.Error("unable to process the document because it is not an object", zap.Any("document", json.RawMessage(originalDoc)))
				notAnObjectTotal.Inc()
				continue
			}
			return total, fmt.Errorf("processing doc: %s", err)
		}
		parseDuration += time.Since(parseStart)

		binaryDocs.B = binary.LittleEndian.AppendUint32(binaryDocs.B, uint32(len(doc)))
		binaryDocs.B = append(binaryDocs.B, doc...)
		for _, meta := range metas {
			binaryMetas.B = marshalAppendMeta(binaryMetas.B, meta)
		}
		total++
	}

	bulkParseDurationSeconds.Observe(parseDuration.Seconds())

	compressor.CompressDocsAndMetas(binaryDocs.B, binaryMetas.B)

	return total, nil
}

func marshalAppendMeta(dst []byte, meta frac.MetaData) []byte {
	metaLenPosition := len(dst)
	dst = append(dst, make([]byte, 4)...)
	dst = meta.MarshalBinaryTo(dst)
	// Metadata length = len(slice after append) - len(slice before append).
	metaLen := uint32(len(dst) - metaLenPosition - 4)
	// Put metadata length before metadata bytes.
	binary.LittleEndian.PutUint32(dst[metaLenPosition:], metaLen)
	return dst
}

func (i *Ingestor) getProcessor() *processor {
	procEface := i.procPool.Get()
	if procEface != nil {
		// The proc already initialized with current ingestor config, so we don't need to reinit it.
		return procEface.(*processor)
	}
	index := rand.Uint64() % consts.IngestorMaxInstances
	return newBulkProcessor(i.config.MappingProvider.GetMapping(), i.tokenizers, i.config.AllowedTimeDrift, i.config.FutureAllowedTimeDrift, index)
}

func (i *Ingestor) putProcessor(proc *processor) {
	i.procPool.Put(proc)
}
