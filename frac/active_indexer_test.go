package frac_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
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

	return indexer.NewProcessor(mapping, tokenizers, 0, 0)
}

func BenchmarkIndexer(b *testing.B) {
	logger.SetLevel(zapcore.FatalLevel)
	idx, stop := frac.NewActiveIndexer(8, 8)
	defer stop()

	allLogs, err := readFileAllAtOnce(filepath.Join(common.TestDataDir, "k8s.logs"))
	readers := splitLogsToBulks(allLogs, 1000)
	assert.NoError(b, err)

	active := frac.NewActive(
		filepath.Join(b.TempDir(), "test"),
		idx,
		storage.NewReadLimiter(1, nil),
		cache.NewConcurrentCache[[]byte](nil, nil),
		cache.NewConcurrentCache[[]byte](nil, nil),
		&frac.Config{},
		testSkipMaskProvider{},
	)

	processor := getTestProcessor()

	for b.Loop() {
		b.StopTimer()
		bulks := make([][]byte, 0, len(readers))
		for _, readNext := range readers {
			_, _, meta, _ := processor.ProcessBulk(time.Now(), nil, nil, readNext)
			bulks = append(bulks, storage.CompressWalBlock(meta, nil, 3))
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

func TestIndexerDuplicateIDWithinBulk(t *testing.T) {
	idx, stop := frac.NewActiveIndexer(1, 1)
	defer stop()

	active := frac.NewActive(
		filepath.Join(t.TempDir(), "test"),
		idx,
		storage.NewReadLimiter(1, nil),
		cache.NewConcurrentCache[[]byte](nil, nil),
		cache.NewConcurrentCache[[]byte](nil, nil),
		&frac.Config{},
		testSkipMaskProvider{},
	)
	defer active.Release()

	processor := getTestProcessor()
	docs := [][]byte{
		[]byte(`{"status":"200","request":"first"}`),
		[]byte(`{"status":"500","request":"second with the same id"}`),
		[]byte(`{"status":"404","request":"third"}`),
	}

	var metas []indexer.MetaData
	for _, doc := range docs {
		_, docMetas, err := processor.ProcessDoc(doc, time.Now())
		require.NoError(t, err)
		for _, m := range docMetas {
			m.Tokens = slices.Clone(m.Tokens)
			metas = append(metas, m)
		}
	}
	metas[1].ID = metas[0].ID

	var raw []byte
	for _, m := range metas {
		lenPos := len(raw)
		raw = append(raw, 0, 0, 0, 0)
		raw = m.MarshalBinaryTo(raw)
		binary.LittleEndian.PutUint32(raw[lenPos:], uint32(len(raw)-lenPos-4))
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	idx.Index(active, storage.CompressWalBlock(raw, nil, 3), &wg, stopwatch.New())
	wg.Wait()

	assert.Equal(t, uint32(2), active.Info().DocsTotal)
	assert.Equal(t, active.Info().DocsTotal, active.MIDs.Len()-1)
	assert.Len(t, active.GetAllDocuments(), int(active.Info().DocsTotal))
}
