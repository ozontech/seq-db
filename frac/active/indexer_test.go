package active

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tokenizer"
)

func readFileAllAtOnce(filename string) ([][]byte, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(content, []byte{'\n'})
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return lines, nil
}

func splitLogsToBulks(data [][]byte, bulkSize int) []func() ([]byte, error) {
	funcs := []func() ([]byte, error){}
	for len(data) > 0 {
		size := min(len(data), bulkSize)
		funcs = append(funcs, testBufReader(data[0:size]))
		data = data[size:]
	}
	return funcs
}

func testBufReader(data [][]byte) func() ([]byte, error) {
	orig := data
	return func() ([]byte, error) {
		if len(data) == 0 {
			data = orig
			return nil, nil
		}
		line := data[0]
		data = data[1:]
		return line, nil
	}
}

func getTestProcessor() *indexer.Processor {
	mapping := seq.Mapping{
		"clientip": seq.NewSingleType(seq.TokenizerTypeKeyword, "clientip", 1024),
		"request":  seq.NewSingleType(seq.TokenizerTypeText, "request", 1024),
		"status":   seq.NewSingleType(seq.TokenizerTypeKeyword, "status", 1024),
		"size":     seq.NewSingleType(seq.TokenizerTypeKeyword, "size", 1024),
	}

	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(1024, false, true, 8192),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(1024, false, true),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(1024, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}

	return indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
}

func BenchmarkIndexer(b *testing.B) {
	logger.SetLevel(zapcore.FatalLevel)
	idx, stop := NewIndexer(8, 8)
	defer stop()

	allLogs, err := readFileAllAtOnce(filepath.Join(common.TestDataDir, "k8s.logs"))
	readers := splitLogsToBulks(allLogs, 1000)
	assert.NoError(b, err)

	active := New(
		filepath.Join(b.TempDir(), "test"),
		idx,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		&frac.Config{},
	)

	processor := getTestProcessor()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bulks := make([][]byte, 0, len(readers))
		for _, readNext := range readers {
			_, _, meta, _ := processor.ProcessBulk(time.Now(), nil, nil, readNext)
			bulks = append(bulks, storage.CompressDocBlock(meta, nil, 3))
		}
		b.StartTimer()

		wg := sync.WaitGroup{}
		for _, meta := range bulks {
			wg.Add(1)
			idx.Index(active, meta, &wg, stopwatch.New())
		}
		wg.Wait()
	}
}

func defaultSealingParams() frac.SealParams {
	const minZstdLevel = 1
	return frac.SealParams{
		IDsZstdLevel:           minZstdLevel,
		LIDsZstdLevel:          minZstdLevel,
		TokenListZstdLevel:     minZstdLevel,
		DocsPositionsZstdLevel: minZstdLevel,
		TokenTableZstdLevel:    minZstdLevel,
		DocBlocksZstdLevel:     minZstdLevel,
		DocBlockSize:           128 * int(units.KiB),
	}
}

func BenchmarkFullWrite(b *testing.B) {
	logger.SetLevel(zapcore.FatalLevel)
	idx, stop := NewIndexer(8, 8)
	defer stop()

	allLogs, err := readFileAllAtOnce(filepath.Join(common.TestDataDir, "k8s.logs"))
	readers := splitLogsToBulks(allLogs, 1000)
	assert.NoError(b, err)

	processor := getTestProcessor()

	n := 2
	allDocs := make([][]byte, 0, len(readers)*n)
	allMeta := make([][]byte, 0, len(readers)*n)

	for range n {
		for _, readNext := range readers {
			_, docs, meta, _ := processor.ProcessBulk(time.Now(), nil, nil, readNext)
			allDocs = append(allDocs, storage.CompressDocBlock(docs, nil, 1))
			allMeta = append(allMeta, storage.CompressDocBlock(meta, nil, 1))
		}
	}

	params := defaultSealingParams()

	for b.Loop() {
		active := New(
			filepath.Join(b.TempDir(), "test"),
			idx,
			storage.NewReadLimiter(1, nil),
			cache.NewCache[[]byte](nil, nil),
			cache.NewCache[[]byte](nil, nil),
			&frac.Config{SkipSortDocs: true},
		)

		wg := sync.WaitGroup{}
		for i, meta := range allMeta {
			wg.Add(1)
			go func() {
				err := active.Append(allDocs[i], meta, &wg)
				assert.NoError(b, err)
			}()
		}
		wg.Wait()

		src, err := NewSealingSource(active, params)
		require.NoError(b, err)
		sealed, err := sealing.Seal(src, params)
		require.NoError(b, err)
		assert.Greater(b, int(sealed.Info.DocsTotal), 0)
		active.Release()
	}
}
