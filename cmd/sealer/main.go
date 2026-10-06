package main

// Standalone sealer for the V3 fraction format; built against the era
// commit of that format: testdata/legacy/seal-fraction.sh places this
// file into a checked-out worktree of that commit and runs it there.
// Do not build it in the main module: the era code it compiles against
// is older than the current one.
//
// Differs from the v4/v5 sealer (testdata/legacy/sealer/main.go): the
// sealing package lives in frac/sealed/sealing, and SealParams has no
// LIDBlockSize/TokenBlockSize fields.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/node"
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
	var mappingPath, fracName string
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--mapping":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "--mapping requires a value")
				os.Exit(2)
			}
			i++
			mappingPath = args[i]
		case strings.HasPrefix(arg, "--mapping="):
			mappingPath = strings.TrimPrefix(arg, "--mapping=")
		default:
			fracName = arg
		}
	}
	if fracName == "" {
		fmt.Fprintln(os.Stderr, "usage: sealer <frac-base-name> [--mapping=mapping.yaml] < docs.jsonl")
		os.Exit(2)
	}

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

	mapping := defaultMapping()
	if mappingPath != "" {
		data, err := os.ReadFile(mappingPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot read mapping: %s\n", err)
			os.Exit(2)
		}
		mapping, err = seq.ReadMapping(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot parse mapping: %s\n", err)
			os.Exit(2)
		}
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(20, false, true, 100),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(512, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
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

func defaultMapping() seq.Mapping {
	return seq.Mapping{
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message": seq.NewSingleType(seq.TokenizerTypeText, "", 0),
	}
}
