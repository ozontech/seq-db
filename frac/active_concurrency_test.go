package frac

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tests/common"
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

	tmpDir := common.CreateTempDir()
	defer common.RemoveDir(tmpDir)
	baseName := filepath.Join(tmpDir, "test_fraction")

	activeIndexer := NewActiveIndexer(numIndexWorkers, 1000)
	activeIndexer.Start()
	defer activeIndexer.Stop()

	fraction := NewActive(
		baseName,
		activeIndexer,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		&Config{},
	)

	mapping := seq.Mapping{
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
		"message": seq.NewSingleType(seq.TokenizerTypeText, "", 100),
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 20),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(1024, false, true, 8192),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(1024, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}

	bulksPerWriter := len(bulks) / numWriters

	writeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writersGroup, writeCtx := errgroup.WithContext(writeCtx)

	for writerId := 0; writerId < numWriters; writerId++ {
		start := writerId * bulksPerWriter
		end := start + bulksPerWriter

		writerBulks := bulks[start:end]

		writersGroup.Go(func() error {
			wg := sync.WaitGroup{}
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

				proc := indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
				compressor := indexer.GetDocsMetasCompressor(3, 3)
				_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
				if err != nil {
					return fmt.Errorf("writer %d: processing bulk failed: %w", writerId, err)
				}

				compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
				docsBlock, metasBlock := compressor.DocsMetas()

				wg.Add(1)
				err = fraction.Append(docsBlock, metasBlock, &wg)
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

					_, err = fraction.Search(context.Background(), searchParams)
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

	ctx := context.Background()
	readersGroup, ctx := errgroup.WithContext(ctx)

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
				random := rand.IntN(3)
				switch random {
				case 0:
					query = "message:request"
					filter = func(doc *testDoc) bool {
						return strings.Contains(doc.message, "request")
					}
				case 1:
					query = "service:gateway"
					filter = func(doc *testDoc) bool {
						return doc.service == "gateway"
					}
				case 2:
					query = "level:2"
					filter = func(doc *testDoc) bool {
						return doc.level == 2
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

				fetchedResult, err := fraction.Fetch(ctx, qpr.IDs.IDs())
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
					if (docs[i].timestamp.Before(queryTime) || docs[i].timestamp.Equal(queryTime)) && filter(&docs[i]) {
						expectedDocs = append(expectedDocs, docs[i].doc)
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

	err = readersGroup.Wait()
	assert.NoError(t, err, "concurrent queries should complete without errors")
}

type testDoc = struct {
	doc       string
	message   string
	service   string
	level     int
	timestamp time.Time
}

func generatesMessages(numMessages int, bulkSize int) ([]testDoc, [][]string, time.Time, time.Time) {
	services := []string{"gateway", "proxy", "scheduler"}
	messages := []string{
		"request started", "request completed", "processing timed out",
		"processing data", "processing failed", "processing retry",
	}

	fromTime := time.Date(2000, 1, 1, 13, 0, 0, 0, time.UTC)
	var toTime time.Time

	docs := make([]testDoc, 0, numMessages)

	for i := 0; i < numMessages; i++ {
		service := services[rand.IntN(len(services))]
		message := messages[rand.IntN(len(messages))]
		level := rand.IntN(6)
		timestamp := fromTime.Add(time.Duration(i) * time.Millisecond)
		if i == numMessages-1 {
			toTime = timestamp
		}

		doc := fmt.Sprintf(`{"timestamp":"%s","service":"%s","message":"%s","level":"%d"}`,
			timestamp.Format(time.RFC3339Nano), service, message, level)

		docs = append(docs, testDoc{
			doc:       doc,
			timestamp: timestamp,
			message:   message,
			service:   service,
			level:     level,
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
			bulk[j-i] = docs[j].doc
		}

		bulks = append(bulks, bulk)
	}
	rand.Shuffle(len(bulks), func(i, j int) {
		bulks[i], bulks[j] = bulks[j], bulks[i]
	})
	return docs, bulks, fromTime, toTime
}
