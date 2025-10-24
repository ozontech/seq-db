package frac

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/stretchr/testify/assert"
)

func TestConcurrentAppendAndQuery(t *testing.T) {
	const concurrency = 8
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

	activeIndexer := NewActiveIndexer(concurrency, 1000)
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

		proc := getTestProcessor()
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

	mapping := seq.Mapping{
		"clientip": seq.NewSingleType(seq.TokenizerTypeKeyword, "service", 20),
		"request":  seq.NewSingleType(seq.TokenizerTypeText, "message", 100),
		"status":   seq.NewSingleType(seq.TokenizerTypeKeyword, "level", 20),
	}
	queryAst, err := parser.ParseSeqQL("message:request", mapping)
	assert.NoError(t, err, "failed to parse query")

	searchParams := processor.SearchParams{}
	searchParams.AST = queryAst.Root
	searchParams.From = seq.MID(0)
	searchParams.To = seq.MID(math.MaxUint64)
	searchParams.Limit = 50

	qpr, err := fraction.Search(context.Background(), searchParams)
	assert.NoError(t, err, "search failed")

	fetchedDocs, err := fraction.Fetch(context.Background(), qpr.IDs.IDs())
	assert.NoError(t, err, "fetch docs failed")
}

func getTestProcessor() *indexer.Processor {
	mapping := seq.Mapping{
		"clientip": seq.NewSingleType(seq.TokenizerTypeKeyword, "service", 20),
		"request":  seq.NewSingleType(seq.TokenizerTypeText, "message", 100),
		"status":   seq.NewSingleType(seq.TokenizerTypeKeyword, "level", 20),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(1024, false, true, 8192),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(1024, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}
	return indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
}
