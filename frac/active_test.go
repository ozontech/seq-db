package frac

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
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
	const writers = 8
	const readers = 8
	const queries = 500

	services := []string{"gateway", "proxy", "scheduler"}
	messages := []string{
		"request started", "request completed", "processing timed out",
		"processing data", "processing failed", "processing retry",
	}

	fromTime := time.Date(2000, 1, 1, 13, 0, 0, 0, time.UTC)

	var docs []string

	for i := 0; i < 25000; i++ {
		service := services[rand.IntN(len(services))]
		message := messages[rand.IntN(len(messages))]
		level := rand.IntN(6)
		timestamp := fromTime.Add(time.Duration(i) * time.Millisecond)

		doc := fmt.Sprintf(`{"timestamp":"%s","service":"%s","message":"%s","level":"%d"}`,
			timestamp.Format(time.RFC3339Nano), service, message, level)
		docs = append(docs, doc)
	}

	bulkSize := 100
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

	tmpDir := common.CreateTempDir()
	baseName := filepath.Join(tmpDir, "test_fraction")

	activeIndexer := NewActiveIndexer(writers, 1000)
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
		docsCopy := slices.Clone(bulk)
		rand.Shuffle(len(docsCopy), func(i, j int) {
			docsCopy[i], docsCopy[j] = docsCopy[j], docsCopy[i]
		})

		idx := 0
		readNext := func() ([]byte, error) {
			if idx >= len(docsCopy) {
				return nil, nil
			}
			d := []byte(docsCopy[idx])
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

	for readerID := 0; readerID < readers; readerID++ {
		g.Go(func() error {
			for queryID := 0; queryID < queries; queryID++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				queryAst, err := parser.ParseSeqQL("message:request", mapping)
				if err != nil {
					return fmt.Errorf("reader %d query %d: failed to parse query: %w", readerID, queryID, err)
				}

				searchParams := processor.SearchParams{}
				searchParams.AST = queryAst.Root
				searchParams.From = seq.MID(0)
				searchParams.To = seq.MID(math.MaxUint64)
				searchParams.Limit = 50

				qpr, err := fraction.Search(ctx, searchParams)
				if err != nil {
					return fmt.Errorf("reader %d query %d: search failed: %w", readerID, queryID, err)
				}

				fetchedDocs, err := fraction.Fetch(ctx, qpr.IDs.IDs())
				if err != nil {
					return fmt.Errorf("reader %d query %d: fetch docs failed: %w", readerID, queryID, err)
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
					"reader %d query %d: number of fetched docs should match expected", readerID, queryID)
				assert.Equal(t, expectedDocs, fetchedDocsStrings,
					"reader %d query %d: fetched documents should match expected documents containing 'request' in descending order", readerID, queryID)
			}
			return nil
		})
	}

	err := g.Wait()
	assert.NoError(t, err, "concurrent queries should complete without errors")
}

func getTestProcessor(mapping seq.Mapping) *indexer.Processor {
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(1024, false, true, 8192),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(1024, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}
	return indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
}
