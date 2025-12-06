package active2

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func BenchmarkIndexer(b *testing.B) {
	logger.SetLevel(zapcore.FatalLevel)
	idx := NewIndexer(8)

	allLogs, err := readFileAllAtOnce(filepath.Join(common.TestDataDir, "k8s.logs"))
	readers := splitLogsToBulks(allLogs, 2000)
	assert.NoError(b, err)

	active := New(
		filepath.Join(b.TempDir(), "test"),
		&frac.Config{},
		idx,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
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
			idx.Index(meta, func(index *memIndex, err error) {
				if err != nil {
					logger.Fatal("bulk indexing error", zap.Error(err))
				}
				active.addIndex(index)
				wg.Done()
			})
		}
		// runtime.GC()
		wg.Wait()
	}
}

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
