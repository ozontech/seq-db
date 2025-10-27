package frac

import (
	"context"
	"fmt"
	"math"
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
	const numWriters = 8
	const numReaders = 8
	const numQueries = 500
	const numMessages = 25000
	const bulkSize = 100

	docs, bulks, _, _ := generatesMessages(numMessages, bulkSize)

	tmpDir := common.CreateTempDir()
	defer common.RemoveDir(tmpDir)
	baseName := filepath.Join(tmpDir, "test_fraction")

	activeIndexer := NewActiveIndexer(numWriters, 1000)
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

	wg := sync.WaitGroup{}

	for _, bulk := range bulks {
		idx := 0
		readNext := func() ([]byte, error) {
			if idx >= len(bulk) {
				return nil, nil
			}
			d := []byte(bulk[idx])
			idx++
			return d, nil
		}

		proc := getTestProcessor(mapping)
		compressor := indexer.GetDocsMetasCompressor(3, 3)
		_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
		assert.NoError(t, err, "processing bulk failed")

		compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
		docsBlock, metasBlock := compressor.DocsMetas()

		wg.Add(1)
		err = fraction.Append(docsBlock, metasBlock, &wg)
		assert.NoError(t, err, "appending docs failed")
	}

	wg.Wait()

	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

	for readerId := 0; readerId < numReaders; readerId++ {
		g.Go(func() error {
			for queryID := 0; queryID < numQueries; queryID++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				queryAst, err := parser.ParseSeqQL("message:request", mapping)
				if err != nil {
					return fmt.Errorf("failed to parse query: %w", err)
				}

				searchParams := processor.SearchParams{}
				searchParams.AST = queryAst.Root
				searchParams.From = seq.MID(0)
				searchParams.To = seq.MID(math.MaxUint64)
				searchParams.Limit = 50

				qpr, err := fraction.Search(ctx, searchParams)
				if err != nil {
					return fmt.Errorf("reader %d query %d: search failed: %w", readerId, queryID, err)
				}

				fetchedDocs, err := fraction.Fetch(ctx, qpr.IDs.IDs())
				if err != nil {
					return fmt.Errorf("reader %d query %d: fetch docs failed: %w", readerId, queryID, err)
				}

				fetchedDocsStrings := make([]string, len(fetchedDocs))
				for i, doc := range fetchedDocs {
					fetchedDocsStrings[i] = string(doc)
				}

				var expectedDocs []string
				for i := len(docs) - 1; i >= 0 && len(expectedDocs) < 50; i-- {
					if strings.Contains(docs[i], "request") {
						expectedDocs = append(expectedDocs, docs[i])
					}
				}

				assert.Equal(t, len(expectedDocs), len(fetchedDocsStrings),
					"reader %d query %d: number of fetched docs should match expected", readerId, queryID)
				assert.Equal(t, expectedDocs, fetchedDocsStrings,
					"reader %d query %d: fetched documents should match expected documents containing 'request' in descending order", readerId, queryID)
			}
			return nil
		})
	}

	err := g.Wait()
	assert.NoError(t, err, "concurrent queries should complete without errors")
}

func generatesMessages(numMessages int, bulkSize int) ([]string, [][]string, time.Time, time.Time) {
	services := []string{"gateway", "proxy", "scheduler"}
	messages := []string{
		"request started", "request completed", "processing timed out",
		"processing data", "processing failed", "processing retry",
	}

	fromTime := time.Date(2000, 1, 1, 13, 0, 0, 0, time.UTC)
	var toTime time.Time

	var docs []string

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
		docs = append(docs, doc)
	}

	var bulks [][]string
	for i := 0; i < len(docs); i += bulkSize {
		end := i + bulkSize
		if end > len(docs) {
			end = len(docs)
		}
		bulks = append(bulks, docs[i:end])
	}
	rand.Shuffle(len(bulks), func(i, j int) {
		bulks[i], bulks[j] = bulks[j], bulks[i]
	})
	return docs, bulks, fromTime, toTime
}

func getTestProcessor(mapping seq.Mapping) *indexer.Processor {
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(1024, false, true, 8192),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(1024, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}
	return indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
}
