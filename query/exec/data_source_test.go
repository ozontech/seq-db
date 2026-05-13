package exec

import (
	"math"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
)

func TestFractionDataSource(t *testing.T) {
	ctx := t.Context()

	mapping := seq.Mapping{
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}

	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"service1", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"service1", "level":5}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"service2", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"service2", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"service3", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"service3", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"service4", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"service4", "level":5}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","service":"service5", "level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"service5", "level":3}`,
	}
	fraction := setupFraction(t, mapping, docs)

	queryString := "level:in(3, 5)"

	// TODO: make search params inside FractionDataSource (???)
	queryAst, err := parser.ParseSeqQL(queryString, mapping)
	require.NoError(t, err, "failed to parse query: %s", queryString)
	params := processor.SearchParams{
		AST:   queryAst.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: math.MaxInt32,
	}

	dataSource := NewFractionDatasource(ctx, fraction, params)

	// test combination with executor
	const limit = 5
	limiter := NewLimiter(dataSource, limit)

	outputData := make([]*query.Record, 0)
	for r, _ := limiter.Next(); r != nil; r, _ = limiter.Next() {
		outputData = append(outputData, r)
	}

	for i := range limit {
		// compare only rawData val because ids are not known in advance
		assert.Equal(t, []byte(docs[len(docs)-i-1]), outputData[i].Vals[2].RawData())
	}
}

// TODO: cleanup
func setupFraction(t *testing.T, mapping seq.Mapping, bulks ...[]string) frac.Fraction {
	t.Helper()

	tmpDir := t.TempDir()
	activeIndexer, _ := frac.NewActiveIndexer(4, 10)
	baseName := filepath.Join(tmpDir, "test_fraction")
	active := frac.NewActive(
		baseName,
		activeIndexer,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		&frac.Config{},
		testSkipMaskProvider{},
	)
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
	}

	var wg sync.WaitGroup
	for _, docs := range bulks {
		docsCopy := slices.Clone(docs)
		idx := 0
		readNext := func() ([]byte, error) {
			if idx >= len(docsCopy) {
				return nil, nil
			}
			d := []byte(docsCopy[idx])
			idx++
			return d, nil
		}

		proc := indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
		compressor := indexer.GetDocsMetasCompressor(3, 3)
		_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
		require.NoError(t, err, "processing bulk failed")

		compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
		docsBlock, metasBlock := compressor.DocsMetas()

		wg.Add(1)
		err = active.Append(docsBlock, metasBlock, &wg)
		require.NoError(t, err, "append to active failed")
	}
	wg.Wait()
	return active
}

type testSkipMaskProvider struct{}

func (testSkipMaskProvider) GetIDsIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, error) {
	return node.NewStatic([]uint32{}, false), false, nil
}
func (testSkipMaskProvider) RemoveFrac(_ string) {}
