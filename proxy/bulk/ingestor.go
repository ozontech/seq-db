package bulk

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/network/circuitbreaker"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
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

	compressor := indexer.GetDocsMetasCompressor(i.config.DocsZSTDCompressLevel, i.config.MetasZSTDCompressLevel)
	defer indexer.PutDocMetasCompressor(compressor)

	total, docs, metas, err := i.processDocsToCompressor(compressor, requestTime, readNext)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		logger.Warn("bulk empty request, skipping")
		return 0, nil
	}

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
		New: func() any { return &bytespool.Buffer{B: make([]byte, 0, units.MiB)} },
	}
	binaryMetasPool = sync.Pool{
		New: func() any { return &bytespool.Buffer{B: make([]byte, 0, units.MiB)} },
	}
)

func (i *Ingestor) processDocsToCompressor(
	compressor *indexer.DocsMetasCompressor,
	requestTime time.Time,
	readNext func() ([]byte, error),
) (int, []byte, []byte, error) {
	proc := i.getProcessor()
	defer i.putProcessor(proc)

	binaryDocs := binaryDocsPool.Get().(*bytespool.Buffer)
	defer binaryDocsPool.Put(binaryDocs)
	binaryDocs.Reset()

	binaryMetas := binaryMetasPool.Get().(*bytespool.Buffer)
	defer binaryMetasPool.Put(binaryMetas)
	binaryMetas.Reset()

	var (
		err   error
		total int
	)
	total, binaryDocs.B, binaryMetas.B, err = proc.ProcessBulk(requestTime, binaryDocs.B, binaryMetas.B, readNext)
	if err != nil {
		return 0, nil, nil, err
	}

	compressor.CompressDocsAndMetas(binaryDocs.B, binaryMetas.B)
	docs, metas := compressor.DocsMetas()

	return total, docs, metas, nil
}

func (i *Ingestor) getProcessor() *indexer.Processor {
	procEface := i.procPool.Get()
	if procEface != nil {
		// The proc already initialized with current ingestor config, so we don't need to reinit it.
		return procEface.(*indexer.Processor)
	}
	index := rand.Uint64() % consts.IngestorMaxInstances
	return indexer.NewProcessor(i.config.MappingProvider.GetMapping(), i.tokenizers, i.config.AllowedTimeDrift, i.config.FutureAllowedTimeDrift, index)
}

func (i *Ingestor) putProcessor(proc *indexer.Processor) {
	i.procPool.Put(proc)
}
