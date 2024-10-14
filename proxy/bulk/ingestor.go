package bulk

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/network/circuitbreaker"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/query"
)

var (
	inflightBulks = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "in_flight_queries_total",
		Help:      "",
	})
)

type IngestorConfig struct {
	HotStores   *stores.Stores
	WriteStores *stores.Stores

	BulkCircuit circuitbreaker.Config

	MaxBufferCap           int
	MaxPoolSize            int
	MaxInflightBulks       int
	AllowedTimeDrift       time.Duration
	FutureAllowedTimeDrift time.Duration

	TokenMapping         query.Mapping
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

	pool *Pool[processor]

	inflight atomic.Uint64
	bulks    atomic.Uint64
	docs     atomic.Uint64
	took     atomic.Uint64
}

func NewIngestor(config IngestorConfig, client StorageClient) *Ingestor {
	i := &Ingestor{
		config: config,
		client: client,
		pool: NewPool(config.MaxInflightBulks, func() *processor {
			return newBulkProcessor(
				newIndexer(query.DocStructMapping(config.TokenMapping), config.MaxTokenSize, config.CaseSensitive, config.PartialFieldIndexing),
				config.AllowedTimeDrift,
				config.FutureAllowedTimeDrift,
				rand.Uint64()%consts.IngestorMaxInstances,
				config.MaxBufferCap,
				config.MaxPoolSize,
			)
		}),
	}

	go i.stats()

	return i
}

func (i *Ingestor) stats() {
	for {
		time.Sleep(consts.ProxyBulkStatsInterval)
		if i.bulks.Load() > 0 {
			logger.Info("bulks written",
				zap.Uint64("count", i.bulks.Load()),
				zap.Uint64("docs", i.docs.Load()),
				zap.Uint64("took_ms", i.took.Load()),
				zap.Uint64("inflight_bulks", i.inflight.Load()),
			)
			i.bulks.Store(0)
			i.docs.Store(0)
			i.took.Store(0)
		}
	}
}

func (i *Ingestor) Stop() {
	for j := 0; j < i.pool.Cap(); j++ {
		cache, err := i.pool.RequestItem(context.TODO())
		if err != nil {
			logger.Error("pool cleanup error", zap.Error(err))
		}

		cache.Cleanup()
	}

	i.pool.Stop()
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
)

func (i *Ingestor) ProcessDocuments(ctx context.Context, requestTime time.Time, readNext func() ([]byte, error)) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, consts.BulkTimeout)
	defer cancel()

	inflightBulks.Inc()
	defer inflightBulks.Dec()

	x := i.inflight.Inc()
	defer i.inflight.Dec()

	if int(x) > i.config.MaxInflightBulks {
		rateLimitedTotal.Inc()
		logger.Error(ErrTooManyInflightBulks.Error(),
			zap.Uint64("cur", x),
			zap.Int("limit", i.config.MaxInflightBulks),
		)
		return 0, ErrTooManyInflightBulks
	}

	t := time.Now()
	proc, err := i.pool.RequestItem(ctx)
	if err != nil {
		return 0, err
	}
	defer i.pool.BackToPool(proc)
	defer proc.Cleanup()

	metric.IngestorBulkRequestPoolDurationSeconds.Observe(time.Since(t).Seconds())

	t = time.Now()

	total := 0
	for {
		doc, err := readNext()
		if err != nil {
			return total, fmt.Errorf("reading next document: %s", err)
		}
		if doc == nil {
			break
		}

		if err := proc.Process(doc, requestTime); err != nil {
			return total, fmt.Errorf("processing doc: %s", err)
		}
		total++
	}

	metric.IngestorBulkParseDurationSeconds.Observe(time.Since(t).Seconds())
	if total == 0 {
		logger.Warn("bulk empty request, skipping")
		return 0, nil
	}

	t = time.Now()
	compressor := frac.GetDocsMetasCompressor(i.config.DocsZSTDCompressLevel, i.config.MetasZSTDCompressLevel)
	defer frac.PutDocMetasCompressor(compressor)

	docs, metas := compressor.CompressDocsAndMetas(proc.docs, proc.meta)
	metric.IngestorBulkDocProvideDurationSeconds.Observe(time.Since(t).Seconds())

	t = time.Now()
	if err := i.client.StoreDocuments(ctx, total, docs, metas); err != nil {
		return 0, err
	}
	i.bulks.Inc()
	i.docs.Add(uint64(total))
	docsWritten.Observe(float64(total))
	i.took.Add(uint64(time.Since(t).Milliseconds()))

	return total, nil
}
