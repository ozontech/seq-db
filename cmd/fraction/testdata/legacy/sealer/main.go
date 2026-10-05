package main

// Standalone sealer for the legacy fraction formats (V3..V5): seals JSON
// documents from stdin, one per line. Built against the era commit of a
// format: testdata/legacy/seal-fraction.sh copies this file into a
// checked-out worktree of that commit, patching the few lines that
// differ between the eras (imports, SealParams fields). Do not build it
// in the main module: the era code it compiles against is older than
// the current one.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/sealing"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
)

type stubSkipMaskProvider struct{}

func (stubSkipMaskProvider) GetIDsIteratorByFrac(_ string, _, _ uint32, reverse bool) (node.Node, bool, func() error, error) {
	return node.NewStatic(nil, reverse), false, func() error { return nil }, nil
}

func (stubSkipMaskProvider) GetIDsBitmapByFrac(_ string, _, _ uint32) (*roaring.Bitmap, error) {
	return nil, nil
}

func (stubSkipMaskProvider) RemoveFrac(_ string) {}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: sealer <frac-base-name> < docs.jsonl")
		os.Exit(2)
	}
	fracName := os.Args[1]

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	var readNext func() ([]byte, error)
	readNext = func() ([]byte, error) {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			return readNext()
		}
		return line, nil
	}

	mapping := seq.Mapping{
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message": seq.NewSingleType(seq.TokenizerTypeText, "", 0),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(20, false, true, 100),
	}

	activeIndexer, stopIndexer := frac.NewActiveIndexer(4, 10)
	defer stopIndexer()

	active := frac.NewActive(
		fracName,
		activeIndexer,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		&frac.Config{},
		stubSkipMaskProvider{},
	)

	proc := indexer.NewProcessor(mapping, tokenizers, 0, 0, 0)
	compressor := indexer.GetDocsMetasCompressor(3, 3)

	_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
	if err != nil {
		fmt.Fprintf(os.Stderr, "process bulk: %s\n", err)
		os.Exit(2)
	}

	compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
	docsBlock, metasBlock := compressor.DocsMetas()

	var wg sync.WaitGroup
	wg.Add(1)
	if err := active.Append(docsBlock, metasBlock, &wg); err != nil {
		fmt.Fprintf(os.Stderr, "append: %s\n", err)
		os.Exit(2)
	}
	wg.Wait()

	sealParams := common.SealParams{
		IDsZstdLevel:           1,
		LIDsZstdLevel:          1,
		TokenListZstdLevel:     1,
		DocsPositionsZstdLevel: 1,
		TokenTableZstdLevel:    1,
		DocBlocksZstdLevel:     1,
		LIDBlockSize:           256,
		TokenBlockSize:         128,
		DocBlockSize:           128 * int(units.KiB),
	}

	src, err := frac.NewActiveSealingSource(active, sealParams)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sealing source: %s\n", err)
		os.Exit(2)
	}

	if _, err := sealing.Seal(src, sealParams); err != nil {
		fmt.Fprintf(os.Stderr, "seal: %s\n", err)
		os.Exit(2)
	}

	active.Release()

	files, _ := filepath.Glob(fracName + "*")
	fmt.Fprintf(os.Stderr, "sealed: %v\n", files)
}
