package frac_test

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/sealing"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	testcommon "github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tokenizer"
)

func TestConcurrentAppendAndQuery(t *testing.T) {
	const numIndexWorkers = 8
	const numWriters = 8
	const numReaders = 8
	const numQueries = 500
	const numMessagesPerWriter = 5000
	const bulkSize = 100

	docs, bulks, fromTime, toTime := generatesMessages(numWriters*numMessagesPerWriter, bulkSize)

	tmpDir := testcommon.CreateTempDir()
	fracPath := filepath.Join(tmpDir, "test_fraction")
	defer testcommon.RemoveDir(fracPath)

	activeIndexer, stop := frac.NewActiveIndexer(numIndexWorkers, 1000)
	defer stop()

	active := frac.NewActive(
		fracPath,
		activeIndexer,
		storage.NewReadLimiter(numReaders/2, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		&frac.Config{},
		testSkipMaskProvider{},
	)

	mapping := seq.Mapping{
		"service":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
		"pod":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
		"client_ip": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
		"message":   seq.NewSingleType(seq.TokenizerTypeText, "", 100),
		"level":     seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
		"trace_id":  seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(1024, false, true, 8192),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(1024, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}

	bulksPerWriter := len(bulks) / numWriters

	writersGroup, writeCtx := errgroup.WithContext(t.Context())

	for writerId := 0; writerId < numWriters; writerId++ {
		start := writerId * bulksPerWriter
		end := start + bulksPerWriter

		writerBulks := bulks[start:end]

		writersGroup.Go(func() error {
			wg := sync.WaitGroup{}
			proc := indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
			for _, bulk := range writerBulks {
				select {
				case <-writeCtx.Done():
					return writeCtx.Err()
				default:
				}

				idx := 0
				readNext := func() ([]byte, error) {
					if idx >= len(bulk) {
						return nil, nil
					}
					d := []byte(bulk[idx])
					idx++
					return d, nil
				}

				compressor := indexer.GetDocsMetasCompressor(3, 3)
				_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
				if err != nil {
					return fmt.Errorf("writer %d: processing bulk failed: %w", writerId, err)
				}

				compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
				docsBlock, metasBlock := compressor.DocsMetas()

				wg.Add(1)
				err = active.Append(docsBlock, metasBlock, &wg)
				if err != nil {
					return fmt.Errorf("writer %d: appending docs failed: %w", writerId, err)
				}

				// 20% chance - simply issue a query for race detector to catch something
				if rand.IntN(10) < 2 {
					searchParams := processor.SearchParams{}
					searchParams.Limit = 50
					searchParams.From = seq.MID(0)
					searchParams.To = seq.TimeToMID(toTime)
					ast, err := parser.ParseSeqQL("message:*", mapping)
					if err != nil {
						return err
					}
					searchParams.AST = ast.Root

					_, err = active.Search(t.Context(), searchParams)
					if err != nil {
						return err
					}
				}
			}
			wg.Wait()
			return nil
		})
	}

	err := writersGroup.Wait()
	assert.NoError(t, err, "concurrent writers should complete without errors")

	readTest(t, active, numReaders, numQueries, docs, fromTime, toTime, mapping)

	sealed, err := seal(active)
	assert.NoError(t, err, "sealing error")

	readTest(t, sealed, numReaders, numQueries, docs, fromTime, toTime, mapping)
}

const (
	scheduler = "scheduler"
	database  = "database"
	bus       = "bus"
	proxy     = "proxy"
	gateway   = "gateway"
	kafka     = "kafka"
)

func readTest(t *testing.T, fraction frac.Fraction, numReaders, numQueries int, docs []*testDoc, fromTime, toTime time.Time, mapping seq.Mapping) {
	readersGroup, ctx := errgroup.WithContext(t.Context())

	type queryFilter func(doc *testDoc) bool

	for readerId := 0; readerId < numReaders; readerId++ {
		readersGroup.Go(func() error {
			for q := 0; q < numQueries; q++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				var query string
				var filter queryFilter
				random := rand.IntN(7)
				switch random {
				case 0:
					query = "message:request"
					filter = func(doc *testDoc) bool {
						return strings.Contains(doc.message, "request")
					}
				case 1:
					query = "service:gateway"
					filter = func(doc *testDoc) bool {
						return doc.service == gateway
					}
				case 2:
					query = "level:2"
					filter = func(doc *testDoc) bool {
						return doc.level == 2
					}
				case 3:
					query = "trace_id:trace-1999"
					filter = func(doc *testDoc) bool {
						return doc.traceId == "trace-1999"
					}
				case 4:
					query = "trace_id:trace-500"
					filter = func(doc *testDoc) bool {
						return doc.traceId == "trace-500"
					}
				case 5:
					query = "trace_id:trace-4444"
					filter = func(doc *testDoc) bool {
						return doc.traceId == "trace-4444"
					}
				case 6:
					query = "service:gateway AND level:3"
					filter = func(doc *testDoc) bool {
						return doc.service == gateway && doc.level == 3
					}
				}

				queryAst, err := parser.ParseSeqQL(query, mapping)
				if err != nil {
					return err
				}

				// pick random query time
				queryTime := fromTime.Add(time.Duration(rand.Int64N(int64(toTime.Sub(fromTime)))))

				searchParams := processor.SearchParams{}
				searchParams.AST = queryAst.Root
				searchParams.From = seq.MID(0)
				searchParams.To = seq.TimeToMID(queryTime)
				searchParams.Limit = 50

				qpr, err := fraction.Search(ctx, searchParams)
				if err != nil {
					return fmt.Errorf("search failed: %w", err)
				}

				fetchedResult, err := fraction.Fetch(ctx, qpr.IDs.IDs(), false)
				if err != nil {
					return fmt.Errorf("fetch failed: %w", err)
				}

				fetchedDocs := make([]string, len(fetchedResult))
				for i, doc := range fetchedResult {
					fetchedDocs[i] = string(doc)
				}

				// find docs by time range and provided query filter to match against fetched docs
				var expectedDocs []string
				for i := len(docs) - 1; i >= 0 && len(expectedDocs) < searchParams.Limit; i-- {
					if (docs[i].timestamp.Before(queryTime) || docs[i].timestamp.Equal(queryTime)) && filter(docs[i]) {
						expectedDocs = append(expectedDocs, docs[i].json)
					}
				}

				assert.Equal(t, len(expectedDocs), len(fetchedDocs), "doc count doesn't match")
				if len(expectedDocs) > 0 {
					assert.Equal(t, expectedDocs, fetchedDocs, "docs do not match for query")
				}
			}
			return nil
		})
	}

	err := readersGroup.Wait()
	assert.NoError(t, err, "concurrent queries should complete without errors")
}

type testDoc = struct {
	id        string
	json      string
	message   string
	service   string
	pod       string
	clientIp  string
	level     int
	traceId   string
	timestamp time.Time
}

func generatesMessages(numMessages, bulkSize int) ([]*testDoc, [][]string, time.Time, time.Time) {
	services := []string{gateway, proxy, scheduler, database, bus, kafka}
	messages := []string{
		"request started", "request completed", "processing timed out",
		"processing data", "processing failed", "processing retry",
	}

	fromTime := time.Date(2000, 1, 1, 13, 0, 0, 0, time.UTC)
	var toTime time.Time

	docs := make([]*testDoc, 0, numMessages)

	for i := 0; i < numMessages; i++ {
		service := services[rand.IntN(len(services))]
		message := messages[rand.IntN(len(messages))]
		// populate message with various unique tokens like ids and hex numbers (matches real installation)
		x := rand.IntN(20)
		switch x {
		case 1:
			message += fmt.Sprintf(" %dms", rand.IntN(10000000))
		case 2:
			message += fmt.Sprintf(" %dus", rand.IntN(10000000))
		default:
			message += fmt.Sprintf(" %d", rand.IntN(10000000))
		}

		level := rand.IntN(6)
		timestamp := fromTime.Add(time.Duration(i) * time.Millisecond)
		id := fmt.Sprintf("id-%d", i)
		traceId := fmt.Sprintf("trace-%d", i%5000)
		pod := fmt.Sprintf("pod-%d", i%50)
		clientIp := fmt.Sprintf("192.168.%d.%d", rand.IntN(64), rand.IntN(256))
		if i == numMessages-1 {
			toTime = timestamp
		}

		json := fmt.Sprintf(`{"timestamp":%q,"id": %q, "service":%q,"pod":%q,"client_ip":%q,"message":%q,"trace_id": %q,"level":"%d"}`,
			timestamp.Format(time.RFC3339Nano), id, service, pod, clientIp, message, traceId, level)

		docs = append(docs, &testDoc{
			json:      json,
			timestamp: timestamp,
			id:        id,
			message:   message,
			service:   service,
			pod:       pod,
			clientIp:  clientIp,
			level:     level,
			traceId:   traceId,
		})
	}

	var bulks [][]string
	for i := 0; i < len(docs); i += bulkSize {
		end := i + bulkSize
		if end > len(docs) {
			end = len(docs)
		}

		bulk := make([]string, end-i)
		for j := i; j < end; j++ {
			bulk[j-i] = docs[j].json
		}

		bulks = append(bulks, bulk)
	}
	rand.Shuffle(len(bulks), func(i, j int) {
		bulks[i], bulks[j] = bulks[j], bulks[i]
	})
	return docs, bulks, fromTime, toTime
}

func seal(active *frac.Active) (*frac.Sealed, error) {
	sealParams := common.SealParams{
		IDsZstdLevel:           1,
		LIDsZstdLevel:          1,
		TokenListZstdLevel:     1,
		DocsPositionsZstdLevel: 1,
		TokenTableZstdLevel:    1,
		DocBlocksZstdLevel:     1,
		DocBlockSize:           128 * int(units.KiB),
		LIDBlockSize:           512,
	}
	activeSealingSource, err := frac.NewActiveSealingSource(active, sealParams)
	if err != nil {
		return nil, err
	}
	preloaded, err := sealing.Seal(activeSealingSource, sealParams)
	if err != nil {
		return nil, err
	}

	sealed := frac.NewSealedPreloaded(
		active.BaseFileName,
		preloaded,
		storage.NewReadLimiter(1, nil),
		frac.NewIndexCache(),
		cache.NewCache[[]byte](nil, nil),
		&frac.Config{},
		testSkipMaskProvider{},
	)

	active.Release()
	return sealed, nil
}
